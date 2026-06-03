import json
import base64
import logging
import math
import os
from typing import Any

from app.domain.models import AudioChunkMessage, TranscriptMessage
from app.adapters.db import DatabaseAdapter
from app.adapters.kafka import KafkaProducerAdapter
from app.adapters.whisper import WhisperAdapter
from app.adapters.audio import AudioConverter

logger = logging.getLogger("uvicorn.error.transcribe_usecase")


class TranscribeUseCase:
    """
    Orchestrates the full transcription pipeline for a single audio chunk:
      1. Decode base-64 audio payload.
      2. Convert Opus → WAV (FFmpeg).
      3. Transcribe with Whisper.
      4. Persist transcript to Postgres.
      5. Publish transcript event to Kafka.

    Dependencies are injected via the constructor so they can be swapped or
    mocked in tests without patching module globals.
    """

    def __init__(
        self,
        db: DatabaseAdapter,
        producer: KafkaProducerAdapter,
        whisper: WhisperAdapter,
        audio: AudioConverter,
    ):
        self._db = db
        self._producer = producer
        self._whisper = whisper
        self._audio = audio

    async def execute(self, raw_msg_value: bytes) -> None:
        # ── 1. Parse message ────────────────────────────────────────────
        try:
            payload = json.loads(raw_msg_value.decode("utf-8"))
            msg = AudioChunkMessage(**payload)
        except Exception as e:
            logger.error(f"Invalid audio-chunk message: {e}")
            return

        logger.info(
            f"Processing chunk {msg.chunkId} "
            f"(room={msg.roomId}, peer={msg.peerId}, size={len(msg.audioData)})"
        )

        # ── 2. Decode audio bytes ────────────────────────────────────────
        audio_bytes = base64.b64decode(msg.audioData)
        if not audio_bytes:
            logger.warning("Empty audio payload — skipping.")
            return

        # ── 3. Convert Opus → WAV ────────────────────────────────────────
        try:
            wav_path = self._audio.convert_opus_to_wav(audio_bytes)
        except Exception as e:
            logger.error(f"Audio conversion failed for chunk {msg.chunkId}: {e}")
            return

        # ── 4. Transcribe ────────────────────────────────────────────────
        try:
            segments: list[Any]
            segments, _ = self._whisper.transcribe(wav_path)
            text = " ".join(s.text.strip() for s in segments).strip()
            confidence = (
                min(1.0, max(0.0, math.exp(
                    sum(s.avg_logprob for s in segments) / len(segments)
                )))
                if segments else 1.0
            )
        except Exception as e:
            logger.error(f"Transcription failed for chunk {msg.chunkId}: {e}")
            return
        finally:
            try:
                os.unlink(wav_path)
            except OSError:
                pass

        if not text:
            logger.info(f"No speech detected in chunk {msg.chunkId}.")
            return

        logger.info(f"Transcript for chunk {msg.chunkId}: '{text}' (confidence={confidence:.2f})")

        # ── 5. Persist ───────────────────────────────────────────────────
        try:
            self._db.save_transcript(msg.roomId, msg.peerId, msg.peerName, msg.chunkId, text, confidence)
        except Exception as e:
            logger.error(f"Failed to persist transcript: {e}")

        # ── 6. Publish ───────────────────────────────────────────────────
        event = TranscriptMessage(
            roomId=msg.roomId,
            peerId=msg.peerId,
            peerName=msg.peerName,
            chunkId=msg.chunkId,
            text=text,
            confidence=round(confidence, 4),
            timestamp=msg.timestamp,
        )
        await self._producer.send("transcripts", json.dumps(event.model_dump()).encode())
        logger.info(f"Published transcript event: room={msg.roomId} chunk={msg.chunkId}")
