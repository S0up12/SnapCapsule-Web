import cv2
import hashlib
import os
import shutil
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple
from PIL import Image, ImageOps
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from core.utils.logger import get_logger
from core.utils.media_paths import resolve_preferred_media_path

logger = get_logger("MediaProcessor")
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}

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
    def __init__(self, cache_dir: Path, max_workers: int = 4):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Cache for failed files to prevent CPU loops
        self.failed_cache = set()
        self.transcode_failed_cache = set()
        self._failed_lock = threading.Lock()
        self._transcode_futures: dict[str, Future[bool]] = {}

    def clear_cache(self):
        """Safely clears the thumbnail cache."""
        try:
            # Clear memory cache
            with self._failed_lock:
                self.failed_cache.clear()
            
            # Clear disk cache
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Thumbnail cache cleared.")
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")

    def get_cache_path(self, file_path: Path, size: Tuple[int, int], crop: bool, overlay_path: Optional[str] = None) -> Path:
        mode = "crop" if crop else "fit"
        overlay_sig = f"_overlay_{hashlib.md5(overlay_path.encode('utf-8', errors='ignore')).hexdigest()}" if overlay_path else ""
        unique_str = f"{file_path.absolute()}_{size[0]}x{size[1]}_{mode}{overlay_sig}"
        hash_name = hashlib.md5(unique_str.encode()).hexdigest()
        return self.cache_dir / f"{hash_name}.jpg"

    def get_web_video_path(self, file_path: Path) -> Path:
        try:
            stats = file_path.stat()
            fingerprint = f"{file_path.absolute()}_{stats.st_mtime_ns}_{stats.st_size}"
        except Exception:
            fingerprint = str(file_path.absolute())
        hash_name = hashlib.md5(fingerprint.encode()).hexdigest()
        return self.cache_dir / "web" / f"{hash_name}_web.mp4"

    @staticmethod
    def _transcode_video_for_web(input_path: str, output_path: str) -> bool:
        try:
            source = Path(input_path)
            target = Path(output_path)
            if not source.exists():
                return False

            target.parent.mkdir(parents=True, exist_ok=True)
            command = [
                "ffmpeg",
                "-y",
                "-i",
                input_path,
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
                timeout=180,
            )
            return target.exists() and target.stat().st_size > 0
        except Exception:
            return False

    @staticmethod
    def _generate_thumbnail(file_path: str, cache_path: str, size: Tuple[int, int], crop: bool, overlay_path: Optional[str] = None) -> bool:
        try:
            path = Path(file_path)
            if not path.exists():
                return False
            try:
                if path.stat().st_size == 0:
                    return False
            except Exception:
                return False
            Path(cache_path).parent.mkdir(parents=True, exist_ok=True)

            img = None
            
            # --- 1. IMAGE ---
            if path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.heic']:
                try:
                    with Image.open(file_path) as src:
                        src.load()
                        img = ImageOps.exif_transpose(src).copy()
                except Exception:
                    return False

            # --- 2. VIDEO ---
            elif path.suffix.lower() in ['.mp4', '.mov', '.avi', '.webm']:
                # Strategy A: OpenCV
                cap = None
                try:
                    # Suppress the "moov atom not found" C-level errors
                    with suppress_c_stderr():
                        cap = cv2.VideoCapture(file_path)

                    if cap is not None and cap.isOpened():
                        w = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
                        h = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)

                        if w > 0 and h > 0:
                            ret, frame = cap.read()
                            if ret:
                                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                                img = Image.fromarray(frame_rgb)
                except Exception:
                    pass
                finally:
                    try:
                        if cap is not None:
                            cap.release()
                    except Exception:
                        pass

                # Strategy B: FFmpeg
                if img is None:
                    try:
                        # Suppress FFmpeg command line output
                        cmd = [
                            "ffmpeg", "-y", "-i", file_path, 
                            "-ss", "00:00:00", "-vframes", "1", 
                            "-q:v", "2", cache_path
                        ]
                        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
                        
                        if os.path.exists(cache_path):
                            with Image.open(cache_path) as src:
                                src.load()
                                img = src.copy()
                    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, Exception):
                        pass

            if not img:
                return False

            # --- 3. APPLY OVERLAY ---
            if overlay_path:
                try:
                    if os.path.exists(overlay_path):
                        with Image.open(overlay_path) as overlay_img:
                            overlay_img.load()
                            overlay = overlay_img.convert("RGBA")
                        img = img.convert("RGBA")
                        if overlay.size != img.size:
                            overlay = overlay.resize(img.size, Image.Resampling.LANCZOS)
                        img = Image.alpha_composite(img, overlay)
                        img = img.convert("RGB")
                except Exception:
                    pass 

            # --- 4. RESIZE & SAVE ---
            if img.mode in ("RGBA", "P"): 
                img = img.convert("RGB")
            
            if crop:
                img = ImageOps.fit(img, size, method=Image.Resampling.LANCZOS)
            else:
                img.thumbnail(size, Image.Resampling.LANCZOS)
            
            img.save(cache_path, "JPEG", quality=85)
            return True

        except Exception:
            return False

    async def get_thumbnail(self, file_path: Path, size: Tuple[int, int] = (200, 200), crop: bool = False, overlay_path: Optional[Path] = None) -> Optional[Path]:
        resolved_path = Path(resolve_preferred_media_path(str(file_path)))
        path_str = str(resolved_path.absolute())

        with self._failed_lock:
            if path_str in self.failed_cache:
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
        with self._failed_lock:
            self.failed_cache.add(path_str)
        return None

    def get_web_media_sync(
        self,
        file_path: Path,
        timeout: float | None = None,
    ) -> Optional[Path]:
        resolved_path = Path(resolve_preferred_media_path(str(file_path)))
        if resolved_path.suffix.lower() not in VIDEO_EXTENSIONS:
            return resolved_path if resolved_path.exists() else None

        path_str = str(resolved_path.absolute())
        with self._failed_lock:
            if path_str in self.transcode_failed_cache:
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

        with self._failed_lock:
            self.transcode_failed_cache.add(path_str)
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
        path_str = str(resolved_path.absolute())

        with self._failed_lock:
            if path_str in self.failed_cache:
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
        with self._failed_lock:
            self.failed_cache.add(path_str)
        return None
