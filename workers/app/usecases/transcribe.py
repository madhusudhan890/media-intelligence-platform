import json
import base64
import logging
import math
import os

from app.domain.models import AudioChunkMessage, TranscriptMessage
from app.adapters.db import save_transcript
from app.adapters.kafka import get_kafka_producer
from app.adapters.whisper import get_whisper_model
from app.adapters.audio import convert_opus_to_wav

logger = logging.getLogger("transcribe_usecase")

class TranscribeUseCase:
    def __init__(self):
        self.kafka_producer = None

    async def execute(self, raw_msg_value: bytes):
        try:
            payload = json.loads(raw_msg_value.decode("utf-8"))
            try:
                msg = AudioChunkMessage(**payload)
            except Exception as ve:
                logger.error(f"Invalid message format: {ve}")
                return

            logger.info(f"Processing chunk {msg.chunkId} (size: {len(msg.audioData)} chars) for room {msg.roomId}, peer {msg.peerId}")
            audio_bytes = base64.b64decode(msg.audioData)
            if not audio_bytes:
                logger.warning("Empty audio payload received.")
                return

            try:
                wav_path = convert_opus_to_wav(audio_bytes)
            except Exception as e:
                logger.error(f"Skipping chunk {msg.chunkId} due to conversion failure: {e}")
                return

            try:
                whisper_model = get_whisper_model()
                segments, info = whisper_model.transcribe(wav_path, beam_size=5)
                segments_list = list(segments)
                text = " ".join([s.text.strip() for s in segments_list]).strip()
                
                confidence = 1.0
                if segments_list:
                    avg_logprob = sum([s.avg_logprob for s in segments_list]) / len(segments_list)
                    confidence = min(1.0, max(0.0, math.exp(avg_logprob)))
            except Exception as e:
                logger.error(f"Error during transcription of chunk {msg.chunkId}: {e}")
                return
            finally:
                try:
                    os.unlink(wav_path)
                except:
                    pass

            if not text:
                logger.info(f"No speech detected in chunk {msg.chunkId}. Skipping.")
                return

            logger.info(f"Transcribed text for chunk {msg.chunkId}: '{text}' (Confidence: {confidence:.2f})")

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

            transcript_event = TranscriptMessage(
                roomId=msg.roomId,
                peerId=msg.peerId,
                chunkId=msg.chunkId,
                text=text,
                confidence=round(confidence, 4),
                timestamp=msg.timestamp
            )

            if self.kafka_producer is None:
                self.kafka_producer = await get_kafka_producer()

            event_bytes = json.dumps(transcript_event.model_dump()).encode("utf-8")
            await self.kafka_producer.send_and_wait("transcripts", value=event_bytes)
            logger.info(f"Published transcript event to 'transcripts' topic for room {msg.roomId}, chunk {msg.chunkId}")

        except Exception as e:
            logger.error(f"Unexpected error in TranscribeUseCase: {e}")
