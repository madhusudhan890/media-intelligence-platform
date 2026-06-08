import os
import json
import base64
import asyncio
import logging
import tempfile
import subprocess
import math
from faster_whisper import WhisperModel
from pydantic import ValidationError

from models import AudioChunkMessage, TranscriptMessage
from db import save_transcript
from kafka_client import start_kafka_consumer, get_kafka_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transcription_worker")

# Initialize Whisper model
logger.info("Initializing Whisper model (base, cpu, int8)...")
whisper_model = WhisperModel("base", device="cpu", compute_type="int8")
logger.info("Whisper model initialized successfully.")

# Initialize Kafka producer
kafka_producer = None

def convert_opus_to_wav(audio_data_bytes: bytes) -> str:
    """Writes raw bytes to a temp file and uses ffmpeg to convert to a mono 16kHz WAV file."""
    # Write input bytes to a temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".opus") as temp_in:
        temp_in.write(audio_data_bytes)
        temp_in_path = temp_in.name

    # Create temporary destination path for wav
    temp_out = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
    temp_out_path = temp_out.name
    temp_out.close()

    try:
        # Convert using ffmpeg
        # Since we receive raw RTP packets, they are raw Opus payloads.
        # Try both with and without explicit format flags to be as robust as possible.
        cmd = [
            "ffmpeg", "-y",
            "-f", "opus",
            "-i", temp_in_path,
            "-ar", "16000",
            "-ac", "1",
            temp_out_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if result.returncode != 0:
            # Fallback if raw opus demuxer fails (e.g. if the payload has header packets or is webm/ogg)
            logger.warning(f"FFmpeg raw opus demuxing failed, trying auto-detect: {result.stderr.decode()}")
            cmd_fallback = [
                "ffmpeg", "-y",
                "-i", temp_in_path,
                "-ar", "16000",
                "-ac", "1",
                temp_out_path
            ]
            subprocess.run(cmd_fallback, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return temp_out_path
    except Exception as e:
        logger.error(f"Failed to convert audio via FFmpeg: {e}")
        # Clean up the output wav file since it might be empty/invalid
        try:
            os.unlink(temp_out_path)
        except:
            pass
        raise e
    finally:
        # Always clean up the input temp file
        try:
            os.unlink(temp_in_path)
        except:
            pass

async def process_audio_chunk(raw_msg_value: bytes):
    global kafka_producer
    try:
        # 1. Decode and parse Kafka message
        payload = json.loads(raw_msg_value.decode("utf-8"))
        try:
            msg = AudioChunkMessage(**payload)
        except ValidationError as ve:
            logger.error(f"Invalid message format: {ve}")
            return

        logger.info(f"Processing chunk {msg.chunkId} (size: {len(msg.audioData)} chars) for room {msg.roomId}, peer {msg.peerId}")

        # Decode base64 audio data
        audio_bytes = base64.b64decode(msg.audioData)
        if not audio_bytes:
            logger.warning("Empty audio payload received.")
            return

        # 2. Convert to temporary WAV
        try:
            wav_path = convert_opus_to_wav(audio_bytes)
        except Exception as e:
            logger.error(f"Skipping chunk {msg.chunkId} due to conversion failure: {e}")
            return

        # 3. Run faster-whisper transcription
        try:
            # Transcribe audio file (beam_size=5 is Whisper standard)
            segments, info = whisper_model.transcribe(wav_path, beam_size=5)
            
            segments_list = list(segments)
            text = " ".join([s.text.strip() for s in segments_list]).strip()
            
            # Map avg_logprob to confidence (avg_logprob is log probability, exp(logprob) gives standard probability)
            confidence = 1.0
            if segments_list:
                avg_logprob = sum([s.avg_logprob for s in segments_list]) / len(segments_list)
                confidence = min(1.0, max(0.0, math.exp(avg_logprob)))
        except Exception as e:
            logger.error(f"Error during transcription of chunk {msg.chunkId}: {e}")
            return
        finally:
            # Clean up the wav file
            try:
                os.unlink(wav_path)
            except:
                pass

        if not text:
            logger.info(f"No speech detected in chunk {msg.chunkId}. Skipping.")
            return

        logger.info(f"Transcribed text for chunk {msg.chunkId}: '{text}' (Confidence: {confidence:.2f})")

        # 4. Save transcript to Postgres
        try:
            save_transcript(
                room_id=msg.roomId,
                peer_id=msg.peerId,
                chunk_id=msg.chunkId,
                text=text,
                confidence=confidence
            )
        except Exception as e:
            logger.error(f"Failed to save transcript to database: {e}")
            # We still proceed to publish to Kafka even if DB fails temporarily

        # 5. Publish transcript event to Kafka transcripts topic
        transcript_event = TranscriptMessage(
            roomId=msg.roomId,
            peerId=msg.peerId,
            chunkId=msg.chunkId,
            text=text,
            confidence=round(confidence, 4),
            timestamp=msg.timestamp
        )

        if kafka_producer is None:
            kafka_producer = await get_kafka_producer()

        event_bytes = json.dumps(transcript_event.model_dump()).encode("utf-8")
        await kafka_producer.send_and_wait("transcripts", value=event_bytes)
        logger.info(f"Published transcript event to 'transcripts' topic for room {msg.roomId}, chunk {msg.chunkId}")

    except Exception as e:
        logger.error(f"Unexpected error in process_audio_chunk: {e}")

async def main():
    global kafka_producer
    logger.info("Starting Transcription Worker...")
    
    # Initialize producer early
    kafka_producer = await get_kafka_producer()
    
    # Start consumer loop
    await start_kafka_consumer(
        topic="audio-chunks",
        group_id="transcription-worker-group",
        callback=process_audio_chunk
    )

if __name__ == "__main__":
    asyncio.run(main())
