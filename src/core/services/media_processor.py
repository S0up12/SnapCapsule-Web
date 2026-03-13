import cv2
import hashlib
import os
import shutil
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple

try:
    import pyvips
except ImportError:  # pragma: no cover - runtime dependency in container
    pyvips = None

from core.utils.logger import get_logger
from core.utils.media_paths import resolve_preferred_media_path

logger = get_logger("MediaProcessor")
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}
FFMPEG_THUMBNAIL_TIMEOUT_SECONDS = 5
FFMPEG_WEB_TRANSCODE_TIMEOUT_SECONDS = 30


def _normalize_overlay_path(overlay_path: Optional[str | Path]) -> Optional[str]:
    if not overlay_path:
        return None
    try:
        return str(Path(overlay_path).absolute())
    except Exception:
        return str(overlay_path)


def build_thumbnail_cache_path(
    cache_dir: Path,
    file_path: Path,
    size: Tuple[int, int],
    crop: bool,
    overlay_path: Optional[str | Path] = None,
) -> Path:
    mode = "crop" if crop else "fit"
    overlay_str = _normalize_overlay_path(overlay_path)
    overlay_sig = (
        f"_overlay_{hashlib.md5(overlay_str.encode('utf-8', errors='ignore')).hexdigest()}"
        if overlay_str
        else ""
    )
    unique_str = f"{file_path.absolute()}_{size[0]}x{size[1]}_{mode}{overlay_sig}"
    hash_name = hashlib.md5(unique_str.encode()).hexdigest()
    return cache_dir / f"{hash_name}.webp"


def build_web_video_cache_path(cache_dir: Path, file_path: Path) -> Path:
    try:
        stats = file_path.stat()
        fingerprint = f"{file_path.absolute()}_{stats.st_mtime_ns}_{stats.st_size}"
    except Exception:
        fingerprint = str(file_path.absolute())
    hash_name = hashlib.md5(fingerprint.encode()).hexdigest()
    return cache_dir / "web" / f"{hash_name}_web.mp4"

@contextmanager
def suppress_c_stderr():
    """
    Redirects C-level stderr to os.devnull to silence 
    noisy libraries like FFmpeg/OpenCV.
    """
    try:
        # Get the file descriptor for stderr
        # Note: In some specialized environments (like pythonw), stderr might not exist.
        if not hasattr(sys.stderr, 'fileno'):
            yield
            return

        stderr_fd = sys.stderr.fileno()
        
        # Save a copy of the original stderr so we can restore it
        saved_stderr_fd = os.dup(stderr_fd)
        
        try:
            # Open the null device
            with open(os.devnull, 'w') as devnull:
                # Replace stderr with null
                os.dup2(devnull.fileno(), stderr_fd)
                yield
        finally:
            # Restore original stderr
            os.dup2(saved_stderr_fd, stderr_fd)
            os.close(saved_stderr_fd)
    except Exception:
        # If anything goes wrong with FDs, just run the code normally
        yield

class MediaProcessor:
    def __init__(self, cache_dir: Path, max_workers: int | None = None):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if max_workers is None:
            max_workers = os.cpu_count() or 4
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Cache for failed files to prevent CPU loops
        self.failed_cache = set()
        self.transcode_failed_cache = set()
        self._failed_lock = threading.Lock()
        self._thumbnail_futures: dict[str, Future[bool]] = {}
        self._transcode_futures: dict[str, Future[bool]] = {}

    @staticmethod
    def _absolute_path_key(file_path: str | Path | None) -> str | None:
        if not file_path:
            return None
        try:
            return str(Path(file_path).absolute())
        except Exception:
            return str(file_path)

    def is_quarantined(self, file_path: str | Path | None) -> bool:
        path_key = self._absolute_path_key(file_path)
        if not path_key:
            return False
        with self._failed_lock:
            return path_key in self.failed_cache

    def quarantine_path(self, file_path: str | Path | None) -> None:
        path_key = self._absolute_path_key(file_path)
        if not path_key:
            return

        with self._failed_lock:
            already_quarantined = path_key in self.failed_cache
            self.failed_cache.add(path_key)
            self.transcode_failed_cache.add(path_key)

        if not already_quarantined:
            logger.warning("Quarantined corrupt media file: %s", path_key)

    def clear_cache(self):
        """Safely clears the thumbnail cache."""
        try:
            # Clear memory cache
            with self._failed_lock:
                self.failed_cache.clear()
                self.transcode_failed_cache.clear()
                self._thumbnail_futures.clear()
                self._transcode_futures.clear()
            
            # Clear disk cache
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Thumbnail cache cleared.")
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")

    @staticmethod
    def _prepare_media_path(
        file_path: Path | None,
        resolve_variants: bool = True,
    ) -> Path | None:
        if not file_path:
            return None

        try:
            candidate = Path(file_path)
        except Exception:
            return None

        if resolve_variants:
            candidate = Path(resolve_preferred_media_path(str(candidate)))

        return candidate

    def _ensure_processable_source(self, file_path: str | Path | None) -> Path | None:
        path_key = self._absolute_path_key(file_path)
        if not path_key or self.is_quarantined(path_key):
            return None

        source = Path(path_key)
        if not source.exists():
            return None

        try:
            if os.path.getsize(source) == 0:
                self.quarantine_path(source)
                return None
        except Exception:
            self.quarantine_path(source)
            return None

        return source

    def get_cache_path(self, file_path: Path, size: Tuple[int, int], crop: bool, overlay_path: Optional[str] = None) -> Path:
        return build_thumbnail_cache_path(self.cache_dir, file_path, size, crop, overlay_path)

    def get_web_video_path(self, file_path: Path) -> Path:
        return build_web_video_cache_path(self.cache_dir, file_path)

    @staticmethod
    def _require_pyvips():
        if pyvips is None:
            raise RuntimeError("pyvips is required for thumbnail generation.")
        return pyvips

    def _load_vips_image(self, file_path: str | Path) -> "pyvips.Image":
        vips = self._require_pyvips()
        image = vips.Image.new_from_file(str(file_path), access="sequential")
        try:
            return image.autorot()
        except Exception:
            return image

    @staticmethod
    def _normalize_vips_bands(image: "pyvips.Image") -> "pyvips.Image":
        if image.bands == 2:
            image = image[:1].bandjoin(image[1])
        elif image.bands > 4:
            image = image[:4]
        return image

    def _resize_vips_image(
        self,
        image: "pyvips.Image",
        size: Tuple[int, int],
        crop: bool,
    ) -> "pyvips.Image":
        image = self._normalize_vips_bands(image)
        width, height = size

        if crop:
            return image.thumbnail_image(width, height=height, crop="centre")

        image = image.thumbnail_image(width)
        if image.height > height:
            image = image.resize(height / image.height)
        return image

    def _composite_overlay(
        self,
        base_image: "pyvips.Image",
        overlay_path: str | None,
    ) -> "pyvips.Image":
        if not overlay_path or not os.path.exists(overlay_path):
            return base_image

        overlay = self._load_vips_image(overlay_path)
        overlay = self._normalize_vips_bands(overlay)
        if overlay.width != base_image.width or overlay.height != base_image.height:
            overlay = overlay.resize(
                base_image.width / overlay.width,
                vscale=base_image.height / overlay.height,
            )

        if not base_image.hasalpha():
            base_image = base_image.bandjoin(255)
        if not overlay.hasalpha():
            overlay = overlay.bandjoin(255)

        return base_image.composite2(overlay, "over")

    def _write_vips_image(self, image: "pyvips.Image", cache_path: str) -> bool:
        image = self._normalize_vips_bands(image)
        image.write_to_file(cache_path, Q=80)
        target = Path(cache_path)
        return target.exists() and target.stat().st_size > 0

    def _vips_image_from_video_frame(self, frame) -> "pyvips.Image":
        vips = self._require_pyvips()
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        height, width, bands = frame_rgb.shape
        return vips.Image.new_from_memory(
            frame_rgb.tobytes(),
            width,
            height,
            bands,
            "uchar",
        )

    def _transcode_video_for_web(self, input_path: str, output_path: str) -> bool:
        source = self._ensure_processable_source(input_path)
        if source is None:
            return False

        try:
            target = Path(output_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            command = [
                "ffmpeg",
                "-y",
                "-i",
                str(source),
                "-threads",
                "1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                "-c:a",
                "aac",
                output_path,
            ]
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=FFMPEG_WEB_TRANSCODE_TIMEOUT_SECONDS,
            )
            return target.exists() and target.stat().st_size > 0
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, Exception):
            self.quarantine_path(source)
            return False

    def _generate_thumbnail(self, file_path: str, cache_path: str, size: Tuple[int, int], crop: bool, overlay_path: Optional[str] = None) -> bool:
        path = self._ensure_processable_source(file_path)
        if path is None:
            return False

        try:
            Path(cache_path).parent.mkdir(parents=True, exist_ok=True)

            image = None
            
            # --- 1. IMAGE ---
            if path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.heic']:
                try:
                    image = self._load_vips_image(path)
                except Exception:
                    self.quarantine_path(path)
                    return False

            # --- 2. VIDEO ---
            elif path.suffix.lower() in ['.mp4', '.mov', '.avi', '.webm']:
                # Strategy A: OpenCV
                cap = None
                try:
                    # Suppress the "moov atom not found" C-level errors
                    with suppress_c_stderr():
                        cap = cv2.VideoCapture(str(path))

                    if cap is not None and cap.isOpened():
                        w = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
                        h = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)

                        if w > 0 and h > 0:
                            ret, frame = cap.read()
                            if ret:
                                image = self._vips_image_from_video_frame(frame)
                except Exception:
                    pass
                finally:
                    try:
                        if cap is not None:
                            cap.release()
                    except Exception:
                        pass

                # Strategy B: FFmpeg
                if image is None:
                    try:
                        # Suppress FFmpeg command line output
                        cmd = [
                            "ffmpeg",
                            "-y",
                            "-ss",
                            "00:00:00",
                            "-i",
                            str(path),
                            "-frames:v",
                            "1",
                            "-c:v",
                            "libwebp",
                            "-update",
                            "1",
                            cache_path,
                        ]
                        subprocess.run(
                            cmd,
                            check=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            timeout=FFMPEG_THUMBNAIL_TIMEOUT_SECONDS,
                        )
                        
                        if os.path.exists(cache_path):
                            image = self._load_vips_image(cache_path)
                    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, Exception):
                        self.quarantine_path(path)
                        return False

            if image is None:
                self.quarantine_path(path)
                return False

            # --- 3. APPLY OVERLAY ---
            if overlay_path:
                try:
                    image = self._composite_overlay(image, overlay_path)
                except Exception:
                    pass 

            # --- 4. RESIZE & SAVE ---
            image = self._resize_vips_image(image, size, crop)
            return self._write_vips_image(image, cache_path)

        except Exception:
            self.quarantine_path(path)
            return False

    async def get_thumbnail(self, file_path: Path, size: Tuple[int, int] = (200, 200), crop: bool = False, overlay_path: Optional[Path] = None) -> Optional[Path]:
        resolved_path = Path(resolve_preferred_media_path(str(file_path)))
        path_str = self._absolute_path_key(resolved_path)
        if path_str is None or self.is_quarantined(path_str):
            return None
        if self._ensure_processable_source(resolved_path) is None:
            return None

        o_path_str = str(overlay_path.absolute()) if overlay_path else None
        cache_path = self.get_cache_path(resolved_path, size, crop, o_path_str)

        try:
            if cache_path.exists() and cache_path.stat().st_size > 0:
                return cache_path
        except Exception:
            pass
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        import asyncio
        loop = asyncio.get_running_loop()
        
        try:
            success = await loop.run_in_executor(
                self.executor,
                self._generate_thumbnail,
                path_str,
                str(cache_path),
                size,
                crop,
                o_path_str,
            )
        except Exception:
            success = False
        
        if success:
            logger.info(f"Thumbnail saved to cache: {cache_path}")
            return cache_path
        self.quarantine_path(path_str)
        return None

    def _finalize_thumbnail_future(
        self,
        cache_key: str,
        source_key: str,
        future: Future[bool],
    ) -> None:
        try:
            success = bool(future.result())
        except Exception:
            success = False

        with self._failed_lock:
            current_future = self._thumbnail_futures.get(cache_key)
            if current_future is future and future.done():
                self._thumbnail_futures.pop(cache_key, None)
        if not success:
            self.quarantine_path(source_key)

    def _finalize_transcode_future(
        self,
        source_key: str,
        future: Future[bool],
    ) -> None:
        try:
            success = bool(future.result())
        except Exception:
            success = False

        with self._failed_lock:
            current_future = self._transcode_futures.get(source_key)
            if current_future is future and future.done():
                self._transcode_futures.pop(source_key, None)
        if not success:
            self.quarantine_path(source_key)

    def queue_thumbnail(
        self,
        file_path: Path | None,
        size: Tuple[int, int] = (400, 400),
        crop: bool = False,
        overlay_path: Optional[Path] = None,
        resolve_variants: bool = True,
    ) -> None:
        resolved_path = self._prepare_media_path(file_path, resolve_variants=resolve_variants)
        if resolved_path is None:
            return
        if not resolved_path.exists() or self._ensure_processable_source(resolved_path) is None:
            return

        source_key = self._absolute_path_key(resolved_path)
        if source_key is None or self.is_quarantined(source_key):
            return

        overlay_str = None
        if overlay_path and overlay_path.exists():
            overlay_str = str(overlay_path.absolute())

        cache_path = self.get_cache_path(resolved_path, size, crop, overlay_str)
        try:
            if cache_path.exists() and cache_path.stat().st_size > 0:
                return
        except Exception:
            pass

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_key = str(cache_path.absolute())

        with self._failed_lock:
            existing_future = self._thumbnail_futures.get(cache_key)
            if existing_future is not None and not existing_future.done():
                return
            future = self.executor.submit(
                self._generate_thumbnail,
                source_key,
                str(cache_path),
                size,
                crop,
                overlay_str,
            )
            self._thumbnail_futures[cache_key] = future

        future.add_done_callback(
            lambda completed_future, ck=cache_key, sk=source_key: self._finalize_thumbnail_future(
                ck,
                sk,
                completed_future,
            )
        )

    def queue_web_media(self, file_path: Path | None, resolve_variants: bool = True) -> None:
        resolved_path = self._prepare_media_path(file_path, resolve_variants=resolve_variants)
        if resolved_path is None:
            return
        if (
            resolved_path.suffix.lower() not in VIDEO_EXTENSIONS
            or not resolved_path.exists()
            or self._ensure_processable_source(resolved_path) is None
        ):
            return

        source_key = self._absolute_path_key(resolved_path)
        if source_key is None or self.is_quarantined(source_key):
            return

        web_path = self.get_web_video_path(resolved_path)
        try:
            if web_path.exists() and web_path.stat().st_size > 0:
                return
        except Exception:
            pass

        web_path.parent.mkdir(parents=True, exist_ok=True)

        with self._failed_lock:
            existing_future = self._transcode_futures.get(source_key)
            if existing_future is not None and not existing_future.done():
                return
            future = self.executor.submit(
                self._transcode_video_for_web,
                source_key,
                str(web_path),
            )
            self._transcode_futures[source_key] = future

        future.add_done_callback(
            lambda completed_future, sk=source_key: self._finalize_transcode_future(
                sk,
                completed_future,
            )
        )

    def queue_precompute(
        self,
        file_path: Path | None,
        overlay_path: Optional[Path] = None,
        size: Tuple[int, int] = (400, 400),
        crop: bool = False,
        resolve_variants: bool = True,
    ) -> None:
        if not file_path:
            return
        self.queue_thumbnail(
            file_path,
            size=size,
            crop=crop,
            overlay_path=overlay_path,
            resolve_variants=resolve_variants,
        )
        self.queue_web_media(file_path, resolve_variants=resolve_variants)

    def get_web_media_sync(
        self,
        file_path: Path,
        timeout: float | None = None,
    ) -> Optional[Path]:
        resolved_path = Path(resolve_preferred_media_path(str(file_path)))
        if resolved_path.suffix.lower() not in VIDEO_EXTENSIONS:
            return resolved_path if resolved_path.exists() else None

        path_str = self._absolute_path_key(resolved_path)
        if path_str is None or self.is_quarantined(path_str):
            return resolved_path if resolved_path.exists() else None
        if self._ensure_processable_source(resolved_path) is None:
            return resolved_path if resolved_path.exists() else None

        web_path = self.get_web_video_path(resolved_path)
        try:
            if web_path.exists() and web_path.stat().st_size > 0:
                return web_path
        except Exception:
            pass

        web_path.parent.mkdir(parents=True, exist_ok=True)

        future: Future[bool] | None = None
        try:
            with self._failed_lock:
                existing_future = self._transcode_futures.get(path_str)
                if existing_future is None or existing_future.done():
                    existing_future = self.executor.submit(
                        self._transcode_video_for_web,
                        path_str,
                        str(web_path),
                    )
                    self._transcode_futures[path_str] = existing_future
                future = existing_future

            success = future.result(timeout=timeout) if timeout is not None else future.result()
        except (TimeoutError, Exception):
            success = False
        finally:
            with self._failed_lock:
                current_future = self._transcode_futures.get(path_str)
                if future is not None and current_future is future and current_future.done():
                    self._transcode_futures.pop(path_str, None)

        if success:
            logger.info(f"Web-safe video saved to cache: {web_path}")
            return web_path

        self.quarantine_path(path_str)
        return resolved_path if resolved_path.exists() else None

    def get_thumbnail_sync(
        self,
        file_path: Path,
        size: Tuple[int, int] = (200, 200),
        crop: bool = False,
        overlay_path: Optional[Path] = None,
        timeout: float | None = None,
    ) -> Optional[Path]:
        resolved_path = Path(resolve_preferred_media_path(str(file_path)))
        path_str = self._absolute_path_key(resolved_path)
        if path_str is None or self.is_quarantined(path_str):
            return None
        if self._ensure_processable_source(resolved_path) is None:
            return None

        o_path_str = str(overlay_path.absolute()) if overlay_path else None
        cache_path = self.get_cache_path(resolved_path, size, crop, o_path_str)

        try:
            if cache_path.exists() and cache_path.stat().st_size > 0:
                return cache_path
        except Exception:
            pass
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            future = self.executor.submit(
                self._generate_thumbnail,
                path_str,
                str(cache_path),
                size,
                crop,
                o_path_str,
            )
            success = future.result(timeout=timeout) if timeout is not None else future.result()
        except (TimeoutError, Exception):
            success = False

        if success:
            logger.info(f"Thumbnail saved to cache: {cache_path}")
            return cache_path
        self.quarantine_path(path_str)
        return None
