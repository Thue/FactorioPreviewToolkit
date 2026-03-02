import json
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast

from src.FactorioPreviewToolkit.shared.shared_constants import constants
from src.FactorioPreviewToolkit.shared.structured_logger import log, log_section


def _write_viewer_config_js(planet_image_links: dict[str, str], planet_names_link: str) -> None:
    """
    Writes a JavaScript file that defines the viewerConfig object.
    This includes preview image URLs and a reference to the planet names JS file.
    """
    from src.FactorioPreviewToolkit.shared.shared_constants import constants

    output_path = constants.PREVIEW_LINKS_FILEPATH
    with log_section("📝 Writing viewerConfig.js..."):
        try:
            with output_path.open("w", encoding="utf-8") as f:
                f.write("const viewerConfig = {\n")
                f.write("  planetPreviewSources: {\n")
                for planet, url in planet_image_links.items():
                    f.write(f'    {planet}: "{url}",\n')
                f.write("  },\n")
                f.write(f'  planetNamesSource: "{planet_names_link}"\n')
                f.write("};\n")
            log.info(f"✅ viewerConfig.js written to: {output_path}")
        except Exception:
            log.error(f"❌ Failed to write viewerConfig.js to: {output_path}")
            raise


def _load_planet_names(min_mtime: float | None = None) -> list[str]:
    """
    Loads the list of planet names from the JSON file generated during preview setup.
    """
    planet_file = constants.PLANET_NAMES_REMOTE_VIEWER_FILEPATH
    _wait_for_file(planet_file, min_mtime=min_mtime)
    with log_section("📄 Loading planet names..."):
        try:
            with planet_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            planets = data.get("planets", [])
            log.info(f"✅ Loaded {len(planets)} planets: {', '.join(planets)}")
            return cast(list[str], planets)
        except Exception:
            log.error("❌ Failed to load or parse planet names JSON file.")
            raise




def _wait_for_file(
    path: Path,
    timeout_in_sec: int = 120,
    poll_interval_sec: float = 0.2,
    min_mtime: float | None = None,
) -> None:
    """
    Waits until a file exists, is non-empty, and has a stable size/mtime.
    This avoids reading/uploading files that are still being written.
    """
    start = time.time()
    stable_checks = 0
    last_signature: tuple[int, float] | None = None

    while True:
        if path.exists():
            stat = path.stat()
            signature = (stat.st_size, stat.st_mtime)

            is_fresh_enough = min_mtime is None or stat.st_mtime >= min_mtime
            if stat.st_size > 0 and is_fresh_enough and signature == last_signature:
                stable_checks += 1
                if stable_checks >= 2:
                    return
            else:
                stable_checks = 0

            last_signature = signature

        if time.time() - start > timeout_in_sec:
            raise TimeoutError(f"Timed out waiting for stable file: {path}")

        time.sleep(poll_interval_sec)


class BaseUploader(ABC):
    """
    Abstract uploader class. Uploads the planet names file and all planet preview images.
    Subclasses must implement upload_single().
    """

    def upload_all(self) -> None:
        """
        Uploads the planet names file and all preview images listed in it.
        Saves resulting download links to a JavaScript config file.
        """
        with log_section("🚀 Uploading preview assets..."):
            run_started_at = time.time()
            planet_names = _load_planet_names(min_mtime=run_started_at)
            planet_names_link = self._upload_planet_names_file(run_started_at)
            planet_image_links = self._upload_planet_images(planet_names, run_started_at)
            _write_viewer_config_js(planet_image_links, planet_names_link)
            log.info("✅ All assets uploaded successfully.")

    def _upload_planet_names_file(self, run_started_at: float) -> str:
        """
        Uploads the planet names JS file and returns its public URL.
        """
        with log_section("📤 Uploading planet names file..."):
            try:
                _wait_for_file(constants.PLANET_NAMES_REMOTE_VIEWER_FILEPATH, min_mtime=run_started_at)
                url = self.upload_single(
                    constants.PLANET_NAMES_REMOTE_VIEWER_FILEPATH,
                    constants.PLANET_NAMES_REMOTE_FILENAME,
                )
                log.info("✅ Planet names uploaded.")
                return url
            except Exception:
                log.error("❌ Failed to upload planet names.")
                raise

    def _upload_planet_images(self, planet_names: list[str], run_started_at: float) -> dict[str, str]:
        """
        Uploads all preview images in parallel and returns a dict of download links.
        """

        def upload_planet(planet: str) -> tuple[str, str]:
            with log_section(f"🌍 Uploading {planet} preview..."):
                image_path = constants.PREVIEWS_OUTPUT_DIR / f"{planet}.png"
                _wait_for_file(image_path, min_mtime=run_started_at)
                url = self.upload_single(image_path, f"{planet}.png")
                log.info(f"✅ {planet} uploaded.")
                return planet, url

        links: dict[str, str] = {}
        if not planet_names:
            return links

        max_workers = min(6, len(planet_names))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(upload_planet, planet): planet for planet in planet_names}
            for future in as_completed(future_map):
                planet = future_map[future]
                try:
                    uploaded_planet, url = future.result()
                    links[uploaded_planet] = url
                except Exception:
                    log.error(f"❌ Failed to upload {planet}.png")
                    raise

        return links

    @abstractmethod
    def upload_single(self, local_path: Path, remote_filename: str) -> str:
        """
        Uploads a single file and returns a public URL.
        """
        ...
