import logging
# pyrefly: ignore [missing-import]
from faster_whisper import WhisperModel

logger = logging.getLogger("whisper_adapter")

whisper_model = None

def get_whisper_model() -> WhisperModel:
    global whisper_model
    if whisper_model is None:
        logger.info("Initializing Whisper model (base, cpu, int8)...")
        whisper_model = WhisperModel("base", device="cpu", compute_type="int8")
        logger.info("Whisper model initialized successfully.")
    return whisper_model
