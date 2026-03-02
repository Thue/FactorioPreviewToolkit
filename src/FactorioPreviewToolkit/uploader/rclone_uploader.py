import json
import subprocess
import threading
from pathlib import Path

from src.FactorioPreviewToolkit.shared.config import Config
from src.FactorioPreviewToolkit.shared.shared_constants import constants
from src.FactorioPreviewToolkit.shared.structured_logger import log, log_section
from src.FactorioPreviewToolkit.uploader.base_uploader import BaseUploader


def _rclone_copy_command(rclone_executable: Path, local_path: Path, remote_target: str) -> list[str]:
    """
    Builds a tuned rclone copy command for small single-file uploads.
    """
    return [
        str(rclone_executable),
        "copy",
        str(local_path),
        remote_target,
        "--no-traverse",
        "--transfers",
        "8",
        "--checkers",
        "16",
        "--ignore-times",
    ]





class RcloneUploader(BaseUploader):
    """
    Rclone-based uploader implementation that copies images to a remote and returns shareable links.
    """

    def __init__(self) -> None:
        self._cache_path = constants.PREVIEWS_OUTPUT_DIR / "rclone_link_cache.json"
        self._cache_lock = threading.Lock()
        self._link_cache = self._load_link_cache()

    def _load_link_cache(self) -> dict[str, str]:
        if not self._cache_path.exists():
            return {}

        try:
            with self._cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except Exception:
            pass

        return {}

    def _save_link_cache(self) -> None:
        with self._cache_path.open("w", encoding="utf-8") as f:
            json.dump(self._link_cache, f, indent=2, sort_keys=True)

    def upload_single(self, local_path: Path, remote_filename: str) -> str:
        """
        Uploads a single file using rclone and returns a shareable link.
        """
        config = Config.get()
        rclone_executable = Config.get().rclone_executable
        remote_name = config.rclone_remote_service
        remote_folder = config.rclone_remote_upload_dir
        remote_target = f"{remote_name}:{remote_folder}"
        full_remote_path = f"{remote_target}/{remote_filename}"

        with log_section(f"☁️ Uploading {local_path.name} to {remote_target}..."):
            try:
                result = subprocess.run(
                    _rclone_copy_command(rclone_executable, local_path, remote_target),
                    check=True,
                    capture_output=True,
                    text=True,
                )

                # Filter out known harmless notices
                for line in result.stderr.splitlines():
                    if "Forced to upload files to set modification times" not in line:
                        log.info(line)

                log.info("✅ Upload complete.")
            except subprocess.CalledProcessError as e:
                log.error("❌ Upload failed.")
                log.error(f"stdout:\n{e.stdout}")
                log.error(f"stderr:\n{e.stderr}")
                raise

        with log_section("🌐 Generating shareable link..."):
            try:
                with self._cache_lock:
                    cached_link = self._link_cache.get(full_remote_path)
                if cached_link:
                    log.info(f"🔗 Shareable URL (cached): {cached_link}")
                    return cached_link

                result = subprocess.run(
                    [rclone_executable, "link", full_remote_path],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                share_url = result.stdout.strip()

                # Handle Dropbox
                if "dropbox.com" in share_url:
                    # Force raw preview link
                    share_url = share_url.replace("www.dropbox.com", "dl.dropboxusercontent.com")
                    share_url = share_url.replace("&dl=0", "")
                    share_url = share_url.replace("&dl=1", "")

                with self._cache_lock:
                    self._link_cache[full_remote_path] = share_url
                    self._save_link_cache()

                log.info(f"🔗 Shareable URL: {share_url}")
                return share_url
            except subprocess.CalledProcessError as e:
                log.error("❌ Failed to generate shareable link.")
                log.error(f"stdout:\n{e.stdout}")
                log.error(f"stderr:\n{e.stderr}")
                raise
