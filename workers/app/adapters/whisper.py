import logging
from typing import Any, Optional

# pyrefly: ignore[missing-import]
from faster_whisper import WhisperModel  # type: ignore[import-untyped]
from app.config import (
    WHISPER_MODEL,
    WHISPER_DEVICE,
    WHISPER_COMPUTE_TYPE,
    WHISPER_LANGUAGE,
    WHISPER_INITIAL_PROMPT,
    WHISPER_VAD_FILTER,
)

logger = logging.getLogger("uvicorn.error.whisper_adapter")


class WhisperAdapter:
    """
    Lazily loads and owns a faster-whisper WhisperModel instance.

    Call `load()` once on application startup; then call `transcribe()` freely.
    Using Optional[WhisperModel] + explicit Any return types keeps static
    analysers (Pyrefly, mypy) quiet even when faster_whisper has no stubs.
    """

    def __init__(
        self,
        model_size: str = WHISPER_MODEL,
        device: str = WHISPER_DEVICE,
        compute_type: str = WHISPER_COMPUTE_TYPE,
        language: Optional[str] = WHISPER_LANGUAGE,
        initial_prompt: Optional[str] = WHISPER_INITIAL_PROMPT,
        vad_filter: bool = WHISPER_VAD_FILTER,
    ) -> None:
        self._model_size = model_size
        self._device = device
        self._compute_type = compute_type
        self._language = language
        self._initial_prompt = initial_prompt
        self._vad_filter = vad_filter
        self._model: Optional[WhisperModel] = None

    def load(self) -> None:
        """Download/load the model weights. Idempotent — safe to call multiple times."""
        if self._model is not None:
            return
        logger.info(
            f"Loading Whisper model '{self._model_size}' "
            f"(device={self._device}, compute={self._compute_type})..."
        )
        self._model = WhisperModel(
            self._model_size,
            device=self._device,
            compute_type=self._compute_type,
        )
        logger.info("Whisper model loaded successfully.")

    def transcribe(self, wav_path: str, beam_size: int = 5) -> tuple[list[Any], Any]:
        """
        Transcribe the WAV file at *wav_path*.

        Returns (segments_list, info) where segments_list is a plain list
        (not a generator) so callers can iterate it multiple times safely.
        The return type is annotated with Any to avoid Pyrefly/mypy errors
        caused by faster_whisper's missing type stubs.
        """
        assert self._model is not None, "WhisperAdapter.load() must be called first."
        segments_gen, info = self._model.transcribe(
            wav_path,
            beam_size=beam_size,
            language=self._language,
            initial_prompt=self._initial_prompt,
            vad_filter=self._vad_filter,
        )
        # Materialise the generator immediately — WhisperModel.transcribe()
        # returns a generator; exhausting it here keeps the WAV file open
        # only for as long as necessary.
        return list(segments_gen), info


whisper_adapter = WhisperAdapter()
