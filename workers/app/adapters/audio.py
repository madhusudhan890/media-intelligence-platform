import os
import tempfile
import subprocess
import logging

logger = logging.getLogger("audio_adapter")

def convert_opus_to_wav(audio_data_bytes: bytes) -> str:
    """Writes raw bytes to a temp file and uses ffmpeg to convert to a mono 16kHz WAV file."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".opus") as temp_in:
        temp_in.write(audio_data_bytes)
        temp_in_path = temp_in.name

    temp_out = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
    temp_out_path = temp_out.name
    temp_out.close()

    try:
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
        try:
            os.unlink(temp_out_path)
        except:
            pass
        raise e
    finally:
        try:
            os.unlink(temp_in_path)
        except:
            pass
