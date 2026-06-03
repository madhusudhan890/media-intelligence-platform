import os
import tempfile
import subprocess
import logging

logger = logging.getLogger("uvicorn.error.audio_adapter")


class AudioConverter:
    """
    Converts raw Opus audio bytes to a 16 kHz mono WAV file using FFmpeg.
    All methods are stateless — the class groups related logic and
    makes the dependency on FFmpeg explicit and mockable in tests.
    """

    SAMPLE_RATE = 16_000
    CHANNELS = 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert_opus_to_wav(self, audio_data: bytes) -> str:
        """
        Write *audio_data* to a temp OGG file, convert to WAV via FFmpeg,
        and return the path of the output WAV file.

        The caller is responsible for deleting the returned file when done.
        Raises on FFmpeg failure.
        """
        in_path = self._write_temp(audio_data, suffix=".ogg")
        out_path = self._make_temp(suffix=".wav")
        try:
            self._run_ffmpeg(in_path, out_path)
            return out_path
        except Exception:
            self._safe_delete(out_path)
            raise
        finally:
            self._safe_delete(in_path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_ffmpeg(self, in_path: str, out_path: str) -> None:
        """Decode the OGG/Opus file to WAV."""
        subprocess.run(
            self._ffmpeg_cmd(in_path, out_path),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )

    def _ffmpeg_cmd(self, in_path: str, out_path: str, fmt: str | None = None) -> list[str]:
        cmd = ["ffmpeg", "-y"]
        if fmt:
            cmd += ["-f", fmt]
        cmd += ["-i", in_path, "-ar", str(self.SAMPLE_RATE), "-ac", str(self.CHANNELS), out_path]
        return cmd

    @staticmethod
    def _write_temp(data: bytes, suffix: str) -> str:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as f:
            f.write(data)
            return f.name

    @staticmethod
    def _make_temp(suffix: str) -> str:
        f = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        f.close()
        return f.name

    @staticmethod
    def _safe_delete(path: str) -> None:
        try:
            os.unlink(path)
        except OSError:
            pass

audio_converter = AudioConverter()
