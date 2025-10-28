#!/usr/bin/env python3
import os
import sys
import time
import json
import shlex
import sqlite3
import logging
import subprocess
import threading
import stat
import shutil
import hashlib
import warnings
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pyudev

# =================== CONFIG ===================

OUTPUT_ROOT = Path("/mnt/md0")
MOUNT_ROOT  = Path("/mnt/auto_media")   # temporary mount points we control
LOG_PATH    = Path("/var/log/auto_transcode.log")
DB_PATH     = Path("/var/lib/auto_transcode/state.sqlite3")
DB_LOCK = threading.RLock()
HASH_CHUNK_MB = 16  # read first/last 16MB (fast & robust)

FS_CACHE: dict[str, str] = {}
FS_CACHE_TTL = 600  # seconds
FS_CACHE_TIME: dict[str, float] = {}
FS_PROBE_MOUNT = False  # set True only if your system regularly misses fs type


# Filesystems we consider fair game
FS_WHITELIST = {"exfat", "vfat", "ntfs", "ntfs3", "ext4", "ext3", "ext2", "xfs", "btrfs", "udf", "iso9660"}

# How often to rescan for already-present/missed devices.
# Set to 0 to disable the watchdog.
RESCAN_INTERVAL = 10
RESCAN_COOLDOWN_SEC = 300     # don't re-trigger the same node within 5 minutes

# --- Permissions for output content ---
OWNER_USER = None          # e.g. "raidowl" or None to leave as-is
OWNER_GROUP = None         # e.g. "media"   or None to leave as-is
DIR_MODE   = 0o755         # rwx r-x r-x
FILE_MODE  = 0o644         # rw- r-- r--

# Encode settings (tweak as you like)
FFMPEG_BASE = [
    "ffmpeg", "-hide_banner", "-nostdin", "-y",
    "-i", None,                        # input placeholder
    # map only primary video + first audio (if present); drop subs/data/timecode
    "-map", "0:v:0", "-map", "0:a:0?",
    "-sn", "-dn",
    # codecs
    "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
    # If you want maximum compatibility (8-bit 4:2:0), uncomment:
    # "-pix_fmt", "yuv420p",
    "-c:a", "aac", "-b:a", "192k",
    "-movflags", "+faststart",
    "-max_muxing_queue_size", "4096",
    None                               # output placeholder
]

# Consider these “video” files in general storage
GENERAL_VIDEO_EXTS = {
    ".mp4",".mov",".mkv",".avi",".mpg",".mpeg",".mts",".m2ts",".ts",".wmv",".m4v",".vob"
}

# Minimum file size (MB) to consider as “real” video file to transcode
MIN_MB = 100

# Niceness/IO priority for ffmpeg (optional)
NICE_PREFIX = ["nice", "-n", "10", "ionice", "-c2", "-n", "7"]

# Optional: permanently ignore these physical devices by ID_SERIAL or ID_WWN
DENY_SERIALS = {
    # Example values; run `udevadm info -q property -n /dev/sda | egrep 'ID_SERIAL|ID_WWN'`
    # "Samsung_SSD_860_EVO_1TB_S3Z9NB0K123456X",
    # "wwn-0x5002538d40f4abcd",
}
# Optional: deny specific devices by serial substring match (case-insensitive)
# Leave empty if you don't want to exclude by serial
EXCLUDE_SERIALS = []
# =============================================

warnings.filterwarnings("ignore", category=DeprecationWarning)


def setup_logging(log_path: str | None = None, level=logging.DEBUG):
    """
    Configure a clean, quiet logger for the autoencoder service.

    - Prints concise INFO lines to console (and optional log file)
    - Suppresses noisy debug/warning messages from dependencies
    """
    log = logging.getLogger("autoencoder")
    log.setLevel(level)
    log.propagate = False  # prevent duplicate prints

    # Remove old handlers if script restarts in same process
    if log.handlers:
        for h in list(log.handlers):
            log.removeHandler(h)

    # ---- Formatter ----
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # ---- Console output ----
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    log.addHandler(console)

    # ---- Optional file output ----
    if log_path:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)

    # ---- Quiet down dependencies ----
    for noisy in ("pyudev", "urllib3", "requests", "ffmpeg", "chardet"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log


log = setup_logging(str(LOG_PATH))


def run(cmd, check=True, capture=False, env=None):
    """Run a command, log at DEBUG, optionally capture stdout/stderr."""
    if isinstance(cmd, list):
        cmd_list = cmd
    else:
        cmd_list = shlex.split(cmd)
    logging.getLogger("autoencoder").debug("RUN: %s", " ".join(shlex.quote(x) for x in cmd_list))
    return subprocess.run(cmd_list, check=check, text=True,
                          capture_output=capture, env=env)


def run_quiet(cmd, check=False):
    """Run a command with all stdio redirected to /dev/null."""
    if isinstance(cmd, list):
        cmd_list = cmd
    else:
        cmd_list = shlex.split(cmd)
    return subprocess.run(cmd_list, check=check, stdin=subprocess.DEVNULL,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)


def apply_perms(path: Path, is_dir: bool):
    try:
        if OWNER_USER is not None or OWNER_GROUP is not None:
            shutil.chown(path, user=OWNER_USER, group=OWNER_GROUP)
    except Exception as e:
        logging.getLogger("autoencoder").debug("chown failed for %s: %s", path, e)

    try:
        mode = DIR_MODE if is_dir else FILE_MODE
        os.chmod(path, mode)
    except Exception as e:
        logging.getLogger("autoencoder").debug("chmod failed for %s: %s", path, e)


def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with DB_LOCK:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed (
                id INTEGER PRIMARY KEY,
                src TEXT,
                size INTEGER,
                mtime INTEGER,
                encoded_path TEXT,
                when_ts INTEGER
            )
            """
        )
        # --- Migrations for fingerprinting ---
        try: conn.execute("ALTER TABLE processed ADD COLUMN mtime_ns INTEGER")
        except sqlite3.OperationalError: pass
        try: conn.execute("ALTER TABLE processed ADD COLUMN ctime_ns INTEGER")
        except sqlite3.OperationalError: pass
        try: conn.execute("ALTER TABLE processed ADD COLUMN hash TEXT")
        except sqlite3.OperationalError: pass
        try: conn.execute("ALTER TABLE processed ADD COLUMN src_basename TEXT")
        except sqlite3.OperationalError: pass
        try: conn.execute("ALTER TABLE processed ADD COLUMN dev_id TEXT")
        except sqlite3.OperationalError: pass
        try: conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_processed_hash_size ON processed(hash, size)")
        except sqlite3.OperationalError: pass

        conn.commit()
    return conn


def is_processed_by_hash(conn, size: int, digest: str) -> bool:
    with DB_LOCK:
        cur = conn.execute("SELECT 1 FROM processed WHERE hash=? AND size=?", (digest, size))
        return cur.fetchone() is not None

def mark_processed(conn, src_path: Path, out_path: Path, digest: str):
    st = src_path.stat()
    mtime_ns, ctime_ns = file_times_ns(src_path)
    with DB_LOCK:
        conn.execute("""
            INSERT OR REPLACE INTO processed
                (src, src_basename, size, mtime, mtime_ns, ctime_ns, hash, encoded_path, when_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(src_path), src_path.name, st.st_size, int(st.st_mtime),
            mtime_ns, ctime_ns, digest, str(out_path), int(time.time()),
        ))
        conn.commit()



def get_vol_label(devnode: str) -> str:
    """Get a nice label for the output folder."""
    # Try lsblk NAME/LABEL
    try:
        res = subprocess.run(["lsblk", "-no", "LABEL", devnode], check=False, capture_output=True, text=True)
        label = (res.stdout or "").strip()
        if label:
            return label
    except Exception:
        pass
    # Try blkid
    try:
        res = subprocess.run(["blkid", "-o", "value", "-s", "LABEL", devnode], check=False, capture_output=True, text=True)
        label = (res.stdout or "").strip()
        if label:
            return label
    except Exception:
        pass
    # Fallback to device name
    return Path(devnode).name.replace("/", "_")


def mount_device(devnode: str) -> Path | None:
    """Mount block device. Prefer udisksctl; fallback to manual with detected FS."""
    MOUNT_ROOT.mkdir(parents=True, exist_ok=True)

    # Try udisksctl
    try:
        # fully quiet; udisksctl may mount under /media/<user>
        r = subprocess.run(["udisksctl", "mount", "-b", devnode], check=False,
                           stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out = ((r.stdout or "") + (r.stderr or "")).strip()
        for token in out.split():
            if token.startswith("/") and ("/media/" in token or "/run/media/" in token):
                mp = Path(token.strip().strip("."))
                if mp.is_dir():
                    return mp
    except Exception as e:
        logging.getLogger("autoencoder").debug("udisksctl mount failed for %s: %s", devnode, e)

    # Manual mount
    fstype = get_fstype(devnode)
    label = get_vol_label(devnode)
    mp = MOUNT_ROOT / label
    mp.mkdir(parents=True, exist_ok=True)

    try:
        if fstype:
            run_quiet(["mount", "-t", fstype, "-o", "ro", devnode, str(mp)], check=True)
            return mp
        else:
            # Let kernel auto-detect
            run_quiet(["mount", "-o", "ro", devnode, str(mp)], check=True)
            return mp
    except subprocess.CalledProcessError as e:
        # Final try for exFAT if detection lagged
        if fstype != "exfat":
            try:
                run_quiet(["mount", "-t", "exfat", "-o", "ro", devnode, str(mp)], check=True)
                return mp
            except subprocess.CalledProcessError:
                pass
        logging.getLogger("autoencoder").debug("Manual mount failed for %s (fstype=%s): %s", devnode, fstype, e)
        return None


def mount_optical(devnode: str) -> Path | None:
    """
    Mount a DVD/Blu-ray read-only under MOUNT_ROOT/<label>.
    Tries UDF first, then ISO9660. Returns mountpoint or None.
    """
    label = get_vol_label(devnode) or Path(devnode).name
    mp = MOUNT_ROOT / label
    mp.mkdir(parents=True, exist_ok=True)

    # Prefer UDF (most DVDs/BDs), then ISO9660
    for fs in ("udf", "iso9660"):
        try:
            run_quiet(["mount", "-t", fs, "-o", "ro", devnode, str(mp)], check=True)
            return mp
        except subprocess.CalledProcessError:
            continue

    # Last try via udisksctl
    try:
        r = subprocess.run(["udisksctl", "mount", "-b", devnode], check=False,
                           stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out = ((r.stdout or "") + (r.stderr or "")).strip()
        for token in out.split():
            if token.startswith("/") and ("/media/" in token or "/run/media/" in token):
                p = Path(token.strip().strip("."))
                if p.is_dir():
                    return p
    except Exception:
        pass

    logging.getLogger("autoencoder").debug("Optical mount failed for %s", devnode)
    return None


def optical_has_media(devnode: str) -> bool:
    name = Path(devnode).name  # e.g., sr0
    try:
        # udev flag is most reliable
        ctx = pyudev.Context()
        d = pyudev.Device.from_device_file(ctx, devnode)
        if d.properties.get("ID_CDROM_MEDIA") == "1":
            return True
    except Exception:
        pass
    # Fallback to sysfs
    try:
        state = (Path(f"/sys/block/{name}/device/state").read_text().strip())
        if state.lower() in ("running", "ready"):
            return True
    except Exception:
        pass
    return False


def unmount_path(mp: Path):
    # Try udisksctl first
    try:
        run_quiet(["udisksctl", "unmount", "-b", dev_from_mount(mp)], check=False)
        return
    except Exception:
        pass
    # Fallback
    try:
        run_quiet(["umount", str(mp)], check=False)
    except Exception:
        pass


def dev_from_mount(mp: Path) -> str:
    # Find the backing device of a mountpoint
    res = run(["findmnt", "-n", "-o", "SOURCE", str(mp)], check=False, capture=True)
    dev = (res.stdout or "").strip()
    return dev or ""


def humanize_bytes(num):
    for unit in ["B","KB","MB","GB","TB"]:
        if num < 1024.0:
            return f"{num:.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}PB"


SKIP_DIR_NAMES = {".Trashes",".Spotlight-V100",".fseventsd","System Volume Information","$RECYCLE.BIN","FOUND.000","LOST.DIR"}

def collect_general_videos(root: Path):
    vids = []
    walk_stack = [str(root)]
    while walk_stack:
        cur = walk_stack.pop()
        try:
            with os.scandir(cur) as it:
                for e in it:
                    try:
                        name = e.name
                        if e.is_dir(follow_symlinks=False):
                            if name in SKIP_DIR_NAMES or name.startswith("."):
                                continue
                            walk_stack.append(e.path)
                        elif e.is_file(follow_symlinks=False):
                            ext = os.path.splitext(name)[1].lower()
                            if ext in GENERAL_VIDEO_EXTS:
                                sz = e.stat().st_size
                                if sz >= MIN_MB * 1024 * 1024:
                                    vids.append(Path(e.path))
                    except Exception:
                        continue
        except Exception:
            continue
    return vids



def detect_dvd_main_vobs(root: Path):
    """Return list of VOB files likely to be main feature (largest title set)."""
    vts_dir = root / "VIDEO_TS"
    if not vts_dir.is_dir():
        return []
    vobs = sorted(vts_dir.glob("VTS_*_*.VOB"))
    # Group by title (VTS_XX)
    groups = {}
    for v in vobs:
        parts = v.name.split("_")
        if len(parts) >= 3:
            key = parts[1]  # the XX
            groups.setdefault(key, []).append(v)
    # choose the group with the largest total size
    best = []
    best_size = 0
    for k, files in groups.items():
        size = sum(f.stat().st_size for f in files)
        if size > best_size:
            best_size = size
            best = sorted(files)
    return best


def detect_bluray_main_m2ts(root: Path):
    """Return the single largest .m2ts in BDMV/STREAM as main feature."""
    stream_dir = root / "BDMV" / "STREAM"
    if not stream_dir.is_dir():
        return []
    m2ts = [p for p in stream_dir.glob("*.m2ts") if p.is_file()]
    if not m2ts:
        return []
    return [max(m2ts, key=lambda p: p.stat().st_size)]

def build_vob_concat_input(vobs: list[Path]) -> tuple[list[str], Path]:
    """Create a concat-demuxer list file and return ffmpeg args + the temp path."""
    lst = OUTPUT_ROOT / "_tmp_concat" / f"vob_{int(time.time()*1000)}.txt"
    lst.parent.mkdir(parents=True, exist_ok=True)

    def esc(p: Path) -> str:
        return p.as_posix().replace("'", r"'\''")

    with lst.open("w", encoding="utf-8") as f:
        for p in vobs:
            f.write(f"file '{esc(p)}'\n")

    # concat demuxer; -safe 0 allows absolute paths
    return (["-f", "concat", "-safe", "0", "-i", str(lst)], lst)



def transcode_dvd_title(conn, vobs: list[Path], out_dir: Path, base_name: str):
    """Transcode a DVD title composed of multiple VOBs as one input."""
    # Deduce a representative src for hashing (first VOB)
    src0 = vobs[0]
    pstr = src0.as_posix().lower()
    is_optical = True

    out_dir.mkdir(parents=True, exist_ok=True)
    apply_perms(out_dir, is_dir=True)

    st = src0.stat()
    size = sum(v.stat().st_size for v in vobs)  # size of the title set

    # tolerant hash; if partial, synthesize an identity based on disc label + title basename
    digest, partial = safe_file_hash(src0, tolerate_io_errors=is_optical)
    if partial:
        try:
            rel = src0.resolve().relative_to(MOUNT_ROOT)
            disc_label = rel.parts[0] if rel.parts else "optical"
        except Exception:
            disc_label = "optical"
        digest = f"optical:{disc_label}:{base_name}"

    if is_processed_by_hash(conn, size, digest):
        log.info("Skip (already processed by content): %s", base_name)
        return

    date_str = datetime.now().strftime("%Y%m%d")
    out_subdir = out_dir / date_str
    out_subdir.mkdir(parents=True, exist_ok=True)
    apply_perms(out_subdir, is_dir=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_subdir / f"{base_name}_{timestamp}.mp4"

    # Build concat input
    concat_args, needs_cleanup = build_vob_concat_input(vobs)

    log.info("Encoding DVD title (%d VOBs) -> %s", len(vobs), out_path.name)
    try:
        concat_args, lst_path = build_vob_concat_input(vobs)
        cmd = [
            "ffmpeg", "-hide_banner", "-nostdin", "-y",
            "-ignore_unknown",
            "-probesize", "200M", "-analyzeduration", "400M",
            *concat_args,
            "-map", "0:v:0", "-map", "0:a:0?", "-map", "-0:d", "-map", "-0:s", "-map", "-0:t",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-preset", "veryfast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
            "-movflags", "+faststart",
            "-max_muxing_queue_size", "4096",
            str(out_path),
        ]
        full = NICE_PREFIX + cmd
        run(full, check=True, capture=True)

        # mark as processed using the digest and total size
        mtime_ns, ctime_ns = file_times_ns(src0)
        with DB_LOCK:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed
                    (src, src_basename, size, mtime, mtime_ns, ctime_ns, hash, encoded_path, when_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(src0), base_name, size, int(st.st_mtime),
                    mtime_ns, ctime_ns, digest, str(out_path), int(time.time()),
                ),
            )
            conn.commit()

        apply_perms(out_path, is_dir=False)
        log.info("Encoding complete: %s", out_path.name)
    finally:
        # cleanup temp list file if we used one
        if needs_cleanup and "concat" in concat_args:
            try:
                lst_path.unlink(missing_ok=True)
            except Exception:
                pass


def transcode_file(conn, src: Path, out_dir: Path):
    """
    Transcode one file to H.264/AAC MP4 with content-hash dedupe.
    - Uses tolerant hashing on optical media (DVD/BD) so I/O hiccups don't crash.
    - Dedupe key = (hash, size); if hashing is partial on optical, falls back to a stable identity.
    - Maps only video + optional audio (drops timecode/data), adds tolerant ffmpeg flags.
    - Writes outputs world-readable and timestamps filenames to avoid collisions.
    """
    try:
        # Ensure output dirs exist (0755 if you wired apply_perms)
        out_dir.mkdir(parents=True, exist_ok=True)
        apply_perms(out_dir, is_dir=True)

        # Identify optical sources by path (VIDEO_TS or BDMV)
        pstr = src.as_posix().lower()
        is_optical = ("/video_ts/" in pstr) or ("/bdmv/" in pstr)

        st = src.stat()
        size = st.st_size

        # Hash once (tolerant on optical)
        digest, partial = safe_file_hash(src, tolerate_io_errors=is_optical)

        # If hashing was partial on optical, synthesize a stable identity token
        # so we can still dedupe within the same disc/session
        if partial and is_optical:
            try:
                rel = src.resolve().relative_to(MOUNT_ROOT)
                disc_label = rel.parts[0] if rel.parts else "optical"
            except Exception:
                disc_label = "optical"
            digest = f"optical:{disc_label}:{src.name}"

        # Skip if this exact content already processed
        if is_processed_by_hash(conn, size, digest):
            log.info("Skip (already processed by content): %s", src.name)
            return

        # Output path: …/<date>/<basename>_<timestamp>.mp4
        date_str = datetime.now().strftime("%Y%m%d")
        out_subdir = out_dir / date_str
        out_subdir.mkdir(parents=True, exist_ok=True)
        apply_perms(out_subdir, is_dir=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_subdir / f"{src.stem}_{timestamp}.mp4"

        log.info("Encoding %s -> %s", src.name, out_path.name)

        # ffmpeg command: tolerant to minor read/packet errors; map v+a only
        cmd = [
            "ffmpeg", "-hide_banner", "-nostdin", "-y",
            "-fflags", "+discardcorrupt+ignore_unknown",
            "-err_detect", "ignore_err",
            "-i", str(src),
            "-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
            # If your camera PCM has odd layouts, optionally downmix:
            # "-ac", "2",
            "-movflags", "+faststart",
            "-max_muxing_queue_size", "4096",
            str(out_path),
        ]
        full = NICE_PREFIX + cmd

        # Run ffmpeg (capture stderr so failures show in logs)
        run(full, check=True, capture=True)

        # Record success (use the digest we already computed)
        mtime_ns, ctime_ns = file_times_ns(src)
        with DB_LOCK:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed
                    (src, src_basename, size, mtime, mtime_ns, ctime_ns, hash, encoded_path, when_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(src), src.name, size, int(st.st_mtime),
                    mtime_ns, ctime_ns, digest, str(out_path), int(time.time()),
                ),
            )
            conn.commit()

        # World-readable perms on the result
        apply_perms(out_path, is_dir=False)
        log.info("Encoding complete: %s", out_path.name)

    except subprocess.CalledProcessError as e:
        log.error("ffmpeg failed on %s (exit=%s)", src, e.returncode)
        if getattr(e, "stderr", None):
            log.error("ffmpeg stderr:\n%s", e.stderr.strip())
    except Exception as e:
        log.exception("Unexpected error while transcoding %s: %s", src, e)


def handle_mount(conn, devnode: str, mp: Path):
    """Scan mountpoint for content and encode."""
    label = get_vol_label(devnode)
    out_dir = OUTPUT_ROOT / label

    # Optical media?
    dvd_vobs = detect_dvd_main_vobs(mp)
    br_main  = detect_bluray_main_m2ts(mp)

    if dvd_vobs:
        log.info("Detected DVD structure: %s", mp / "VIDEO_TS")
        # Group by title and encode per group
        # We already return the “best” group in detect_dvd_main_vobs(); name it nicely:
        base = (mp.name or "DVD_TITLE")
        transcode_dvd_title(conn, dvd_vobs, OUTPUT_ROOT / get_vol_label(devnode), base_name=base)
        return

    if br_main:
        logging.getLogger("autoencoder").info("Detected Blu-ray structure: %s", (mp / "BDMV/STREAM"))
        for m in br_main:
            transcode_file(conn, m, out_dir)
        return

    # General removable storage: encode each large video file found
    vids = collect_general_videos(mp)
    if not vids:
        logging.getLogger("autoencoder").info("No sizable video files found on %s", mp)
        return

    vids.sort(key=lambda p: p.stat().st_size, reverse=True)
    for v in vids:
        transcode_file(conn, v, out_dir)


def process_device(conn, devnode: str):
    logging.getLogger("autoencoder").info("Processing device: %s", devnode)
    if Path(devnode).name.startswith("sr"):  # optical
        if not optical_has_media(devnode):
            logging.getLogger("autoencoder").debug("No media in %s; skipping", devnode)
            return
        mp = mount_optical(devnode)
    else:
        mp = mount_device(devnode)

    if not mp:
        logging.getLogger("autoencoder").debug("Could not mount %s", devnode)
        return
    try:
        handle_mount(conn, devnode, mp)
    finally:
        unmount_path(mp)


def device_is_optical(device):
    props = device.properties
    if device.device_node and device.device_node.startswith("/dev/sr"):
        return True
    return props.get("ID_CDROM") == "1" or props.get("ID_TYPE") in ("cd", "dvd", "bluray")


def device_is_removable(device):
    props = device.properties
    if props.get("ID_BUS") in ("usb", "mmc"):
        return True
    if props.get("ID_DRIVE_FLASH_SD") == "1":
        return True
    try:
        removable_path = Path(device.sys_path) / "removable"
        if removable_path.is_file():
            return removable_path.read_text().strip() == "1"
    except Exception:
        pass
    return False


def fast_file_hash(path: Path, chunk_mb: int = HASH_CHUNK_MB) -> str:
    """
    Compute a SHA-256 over head+tail chunks (fast, robust for camera files).
    If file is small (< 2*chunk), hash entire file.
    Returns hex digest.
    """
    chunk = chunk_mb * 1024 * 1024
    h = hashlib.sha256()
    size = path.stat().st_size

    with path.open("rb") as f:
        if size <= 2 * chunk:
            while True:
                b = f.read(1024 * 1024)
                if not b: break
                h.update(b)
        else:
            h.update(f.read(chunk))
            f.seek(max(size - chunk, 0))
            h.update(f.read(chunk))
    return h.hexdigest()


def file_times_ns(path: Path) -> tuple[int, int]:
    st = path.stat()
    return (int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))),
            int(getattr(st, "st_ctime_ns", int(st.st_ctime * 1e9))))


def scan_existing(conn):
    """Process present partitions/discs (skip md/root exclusions)."""
    try:
        res = run(["lsblk", "-J", "-o", "NAME,KNAME,TYPE,FSTYPE,MOUNTPOINT"], check=True, capture=True)
        data = json.loads(res.stdout)
        ctx = pyudev.Context()

        def walk(node):
            name = node.get("kname") or node.get("name")
            devnode = f"/dev/{name}" if name else None
            typ = node.get("type")
            fstype = node.get("fstype")
            mnt = node.get("mountpoint")
            if devnode and typ in ("part", "rom"):
                try:
                    d = pyudev.Device.from_device_file(ctx, devnode)
                    if device_is_candidate(d) and (fstype or mnt):
                        if mnt:
                            handle_mount(conn, devnode, Path(mnt))
                        else:
                            _ = wait_for_property(devnode, "ID_FS_TYPE", timeout=6.0, interval=0.3)
                            process_device(conn, devnode)
                except Exception:
                    pass
            for ch in node.get("children", []) or []:
                walk(ch)

        for top in data.get("blockdevices", []) or []:
            walk(top)

    except Exception as e:
        logging.getLogger("autoencoder").warning("Startup scan failed: %s", e)


EXCLUDE_NAMES = set()  # populated at startup


def device_is_candidate(device) -> bool:
    devnode = device.device_node
    if not devnode or not devnode.startswith("/dev/"):
        return False

    # Treat optical drives (/dev/sr*) as candidates regardless of DEVTYPE
    if devnode.startswith("/dev/sr"):
        # Optional: only when media present
        try:
            if optical_has_media(devnode):
                return True
        except Exception:
            pass
        # If you’d rather always try, uncomment next line:
        # return True

    base = dev_root_base(devnode)
    name = dev_basename(devnode)

    if base in EXCLUDE_NAMES or name in EXCLUDE_NAMES:
        return False

    ser = device_serial(device).lower()
    if ser and any(s.lower() in ser for s in EXCLUDE_SERIALS):
        return False

    devtype = (device.properties.get("DEVTYPE") or "").lower()
    if devtype == "rom":
        return True  # optical

    if devtype == "partition":
        fs = get_fstype(devnode)
        if not fs:
            return False
        if fs not in FS_WHITELIST:
            return False
        return True

    # Allow hotplug whole disks (USB); SATA whole-disks are handled via their partitions
    if is_usb_or_hotplug(device):
        return True

    return False


def periodic_rescan(conn, stop_event: threading.Event):
    """
    Periodically scans block devices and processes any eligible partitions
    that udev might have missed (e.g., SATA bays without hotplug events).
    Relies on device_is_candidate() to enforce exclusions and FS checks.
    """
    logging.getLogger("autoencoder").debug(
        "Starting periodic rescan thread (interval=%ss, cooldown=%ss)",
        RESCAN_INTERVAL, RESCAN_COOLDOWN_SEC,
    )

    last_trigger: dict[str, float] = {}
    ctx = pyudev.Context()

    while not stop_event.wait(RESCAN_INTERVAL):
        try:
            res = run(["lsblk", "-J", "-o", "NAME,KNAME,TYPE,FSTYPE"], check=False, capture=True)
            data = json.loads(res.stdout or "{}")

            def walk(node):
                name = node.get("kname") or node.get("name")
                typ  = (node.get("type") or "").lower()
                devnode = f"/dev/{name}" if name else None

                if devnode and typ in ("part", "rom"):
                    now = time.time()
                    if devnode in last_trigger and (now - last_trigger[devnode]) < RESCAN_COOLDOWN_SEC:
                        return
                    try:
                        d = pyudev.Device.from_device_file(ctx, devnode)
                        if device_is_candidate(d):
                            logging.getLogger("autoencoder").info(
                                "Rescan picked candidate: %s (type=%s, fstype=%s)",
                                devnode, typ, (node.get("fstype") or "<unknown>").lower(),
                            )
                            process_device(conn, devnode)
                            last_trigger[devnode] = now
                    except Exception as e:
                        if devnode.startswith("/dev/sr"):
                            log.info("Rescan (optical fallback) processing %s", devnode)
                            process_device(conn, devnode)
                            last_trigger[devnode] = now
                            return
                        logging.getLogger("autoencoder").debug("Rescan skip %s due to error: %s", devnode, e)

                for ch in (node.get("children") or []):
                    walk(ch)

            for top in (data.get("blockdevices") or []):
                walk(top)
        except Exception as e:
            logging.getLogger("autoencoder").warning("Rescan error: %s", e)


def _first_nonempty(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    return s if s else None


def get_fstype(devnode: str) -> str | None:
    if not devnode or not devnode.startswith("/dev/"):
        return None

    # cache
    now = time.time()
    if devnode in FS_CACHE and (now - FS_CACHE_TIME.get(devnode, 0)) < FS_CACHE_TTL:
        return FS_CACHE[devnode]

    # 1) udev
    try:
        d = pyudev.Device.from_device_file(pyudev.Context(), devnode)
        fs = _first_nonempty(d.properties.get("ID_FS_TYPE"))
        if fs:
            FS_CACHE[devnode] = fs.lower(); FS_CACHE_TIME[devnode] = now
            return FS_CACHE[devnode]
    except Exception:
        pass

    # 2) lsblk
    try:
        r = run(["lsblk", "-no", "FSTYPE", devnode], check=False, capture=True)
        fs = _first_nonempty(r.stdout)
        if fs:
            FS_CACHE[devnode] = fs.lower(); FS_CACHE_TIME[devnode] = now
            return FS_CACHE[devnode]
    except Exception:
        pass

    # 3) blkid
    try:
        r = run(["blkid", "-p", "-o", "value", "-s", "TYPE", devnode], check=False, capture=True)
        fs = _first_nonempty(r.stdout)
        if fs:
            FS_CACHE[devnode] = fs.lower(); FS_CACHE_TIME[devnode] = now
            return FS_CACHE[devnode]
    except Exception:
        pass

    # 4) optional, expensive mount probe
    if FS_PROBE_MOUNT:
        probe_fs = ["exfat","ntfs3","ntfs","vfat","ext4","xfs","btrfs","udf","iso9660"]
        probe_mp = MOUNT_ROOT / ("_probe_" + Path(devnode).name)
        try:
            probe_mp.mkdir(parents=True, exist_ok=True)
            for fs in probe_fs:
                run_quiet(["mount", "-t", fs, "-o", "ro", devnode, str(probe_mp)], check=False)
                run_quiet(["umount", str(probe_mp)], check=False)
                FS_CACHE[devnode] = fs; FS_CACHE_TIME[devnode] = now
                return fs
        finally:
            run_quiet(["umount", str(probe_mp)], check=False)
            try: probe_mp.rmdir()
            except Exception: pass

    return None

def safe_file_hash(path: Path, chunk_mb: int = HASH_CHUNK_MB, tolerate_io_errors: bool = False, retries: int = 2):
    """
    Like fast_file_hash, but tolerates read errors when tolerate_io_errors=True.
    Returns (digest:str, partial:bool). 'partial' means we skipped some bytes due to I/O.
    Strategy: hash head+tail; on error, retry smaller reads; if still failing, hash what we got.
    """
    chunk = chunk_mb * 1024 * 1024
    h = hashlib.sha256()
    size = path.stat().st_size
    partial = False

    def _read_with_retries(fh, n, where=None):
        nonlocal partial
        attempts = retries + 1
        bs = n
        while attempts > 0:
            try:
                return fh.read(bs)
            except OSError as e:
                if not tolerate_io_errors:
                    raise
                attempts -= 1
                partial = True
                # Back off to smaller block; tiny last resort
                bs = max(64 * 1024, bs // 4)
                time.sleep(0.05)
        # final attempt: try tiny reads repeatedly to fill up to n
        buf = bytearray()
        remaining = n
        while remaining > 0:
            try:
                chunk_bytes = fh.read(min(64 * 1024, remaining))
                if not chunk_bytes:
                    break
                buf.extend(chunk_bytes)
                remaining -= len(chunk_bytes)
            except OSError:
                partial = True
                break
        return bytes(buf)

    with path.open("rb", buffering=0) as f:
        if size <= 2 * chunk:
            # whole-file hash with retries in small blocks
            while True:
                try:
                    b = f.read(1024 * 1024)
                except OSError:
                    if not tolerate_io_errors:
                        raise
                    partial = True
                    # try tiny read and continue
                    try:
                        b = f.read(64 * 1024)
                    except OSError:
                        break
                if not b:
                    break
                h.update(b)
        else:
            # head
            b = _read_with_retries(f, chunk, where="head")
            if b:
                h.update(b)
            # tail
            try:
                f.seek(max(size - chunk, 0))
            except OSError:
                # cannot seek reliably, mark partial
                partial = True
            b = _read_with_retries(f, chunk, where="tail")
            if b:
                h.update(b)

    return h.hexdigest(), partial


def is_filesystem_whitelisted_node(devnode: str) -> bool:
    fs = get_fstype(devnode)
    return bool(fs and fs in FS_WHITELIST)


def main():
    global EXCLUDE_NAMES
    os.umask(0o022)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    MOUNT_ROOT.mkdir(parents=True, exist_ok=True)

    EXCLUDE_NAMES = build_exclude_set()
    logging.getLogger("autoencoder").info("Excluding devices: %s", ", ".join(sorted(EXCLUDE_NAMES)) or "(none)")

    conn = ensure_db()
    scan_existing(conn)

    # udev monitor
    context = pyudev.Context()
    monitor = pyudev.Monitor.from_netlink(context)
    monitor.filter_by("block")
    monitor.start()
    observer = pyudev.MonitorObserver(
        monitor, callback=lambda dev: on_event(conn, (dev.action or "change"), dev)
    )
    observer.start()
    logging.getLogger("autoencoder").info("Auto-transcode daemon watching udev...")

    # Watchdog thread
    stop_event = threading.Event()
    rescan_thread = None
    if RESCAN_INTERVAL and RESCAN_INTERVAL > 0:
        rescan_thread = threading.Thread(target=periodic_rescan, args=(conn, stop_event), daemon=True)
        rescan_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.getLogger("autoencoder").info("Shutting down.")
    finally:
        observer.stop()
        if rescan_thread is not None:
            stop_event.set()
            rescan_thread.join(timeout=2)


def on_event(conn, action, device):
    try:
        if action not in ("add", "change", "bind", "move"):
            return

        devnode = device.device_node
        if not devnode or not devnode.startswith("/dev/"):
            return

        time.sleep(0.2)

        if devnode.startswith("/dev/sr"):
            log.info("Optical event on %s (action=%s) — trying mount", devnode, action)
            process_device(conn, devnode)
            return

        if not device_is_candidate(device):
            # keep quiet unless debugging
            logging.getLogger("autoencoder").debug(
                "Non-candidate node: %s (fstype=%s, devtype=%s)",
                devnode, get_fstype(devnode), device.properties.get("DEVTYPE")
            )
            return

        devtype = device.properties.get("DEVTYPE")
        logging.getLogger("autoencoder").debug("udev event: action=%s node=%s devtype=%s", action, devnode, devtype)

        if devtype == "partition":
            fstype = device.properties.get("ID_FS_TYPE") or wait_for_property(devnode, "ID_FS_TYPE", timeout=10.0, interval=0.3)
            logging.getLogger("autoencoder").info("Candidate partition: %s (fs=%s)", devnode, fstype or "unknown")
            process_device(conn, devnode)
            return

        if devtype in ("disk", "rom"):
            part = wait_for_first_partition(devnode, timeout=15.0, interval=0.4)
            chosen = part or devnode
            if chosen != devnode:
                _ = wait_for_property(chosen, "ID_FS_TYPE", timeout=6.0, interval=0.3)
            logging.getLogger("autoencoder").info("Using node %s for processing (parent %s, action=%s)", chosen, devnode, action)
            process_device(conn, chosen)
    except Exception as e:
        logging.getLogger("autoencoder").exception("on_event error: %s", e)


def has_filesystem(device) -> bool:
    t = device.properties.get("ID_FS_TYPE")
    return bool(t)


def is_usb_or_hotplug(device) -> bool:
    dev = device
    while dev is not None:
        props = dev.properties
        if props.get("ID_BUS") == "usb":
            return True
        if "usb-" in (props.get("ID_PATH") or ""):
            return True
        if props.get("HOTPLUG") == "1":
            return True
        dev = dev.parent
    return False


def is_mdraid_member(device) -> bool:
    props = device.properties
    if props.get("ID_FS_TYPE") == "linux_raid_member":
        return True

    try:
        sys_path = Path(device.sys_path)
        if (sys_path / "md").exists():
            return True
        holders = sys_path / "holders"
        if holders.exists():
            for h in holders.iterdir():
                if h.name.startswith("md"):
                    return True
        slaves = sys_path / "slaves"
        if slaves.exists():
            for s in slaves.iterdir():
                if s.name and s.name != Path(device.device_node or "").name and s.name.startswith(("sd","nvme","mmcblk")):
                    return True
    except Exception:
        pass

    return False


def device_serial_or_wwn(device) -> str | None:
    return device.properties.get("ID_SERIAL") or device.properties.get("ID_WWN")


def should_ignore_device(device) -> bool:
    try:
        if is_mdraid_member(device):
            return True
        ser = device_serial_or_wwn(device)
        if ser and ser in DENY_SERIALS:
            return True
    except Exception:
        pass
    return False

import re

def dev_basename(devnode: str) -> str:
    return Path(devnode).name


def dev_root_base(devnode: str) -> str:
    name = dev_basename(devnode)
    m = re.match(r'^(nvme\d+n\d+|mmcblk\d+|md\d+)(p\d+)?$', name)
    if m:
        return m.group(1)
    m = re.match(r'^(sd[a-z]+)\d*$', name)
    if m:
        return m.group(1)
    return name


def list_md_arrays_and_slaves() -> set[str]:
    names: set[str] = set()
    sys_block = Path("/sys/block")
    for md in sys_block.glob("md*"):
        names.add(md.name)
        for part in md.glob(md.name + "p*"):
            names.add(part.name)
        slaves_dir = md / "slaves"
        if slaves_dir.is_dir():
            for slave in slaves_dir.iterdir():
                base = slave.name
                names.add(base)
                for part in sys_block.glob(base + "*"):
                    names.add(part.name)
    return names


def list_rootfs_block_and_parts() -> set[str]:
    out = run(["findmnt", "-n", "-o", "SOURCE", "/"], check=False, capture=True)
    source = (out.stdout or "").strip()
    names: set[str] = set()
    if not source:
        return names

    def add_base_and_parts(base_name: str):
        names.add(base_name)
        for p in Path("/sys/block").glob(base_name + "*"):
            names.add(p.name)

    if source.startswith("/dev/"):
        base = dev_root_base(source)
        add_base_and_parts(base)
        return names

    try:
        js = run(["lsblk", "-J", "-o", "NAME,KNAME,TYPE,PKNAME"], check=False, capture=True)
        data = json.loads(js.stdout or "{}")
        kname_to_node = {}
        def walk(n, parent=None):
            k = n.get("kname") or n.get("name")
            if k:
                kname_to_node[k] = n
            for ch in n.get("children", []) or []:
                walk(ch, k)
        for top in data.get("blockdevices", []) or []:
            walk(top, None)
        src_name = Path(source).name
        cur = kname_to_node.get(src_name)
        while cur and cur.get("type") not in ("disk",):
            pk = cur.get("pkname")
            cur = kname_to_node.get(pk)
        if cur and cur.get("type") == "disk":
            base = cur.get("kname")
            if base:
                add_base_and_parts(base)
    except Exception:
        pass
    return names


def device_serial(device) -> str:
    return (device.properties.get("ID_SERIAL")
            or device.properties.get("ID_SERIAL_SHORT")
            or "")


def build_exclude_set() -> set[str]:
    excl = set()
    excl |= list_md_arrays_and_slaves()
    excl |= list_rootfs_block_and_parts()
    for dev in Path("/dev").glob("md*"):
        excl.add(dev_basename(str(dev)))
    return excl


def wait_for_property(devnode: str, key: str, timeout: float = 12.0, interval: float = 0.3) -> str | None:
    ctx = pyudev.Context()
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            d = pyudev.Device.from_device_file(ctx, devnode)
            val = d.properties.get(key)
            if val:
                return val
        except Exception:
            pass
        time.sleep(interval)
    return None


def wait_for_first_partition(devnode: str, timeout: float = 12.0, interval: float = 0.25) -> str | None:
    base = Path(devnode).name
    deadline = time.time() + timeout
    while time.time() < deadline:
        parts = sorted([*Path("/dev").glob(base + "[0-9]*"), *Path("/dev").glob(base + "p[0-9]*")], key=lambda p: p.name)
        for c in parts:
            r = run(["blkid", "-p", "-o", "value", "-s", "TYPE", str(c)], check=False, capture=True)
            if _first_nonempty(r.stdout):
                return str(c)
        if parts:
            return str(parts[0])
        time.sleep(interval)
    return None



if __name__ == "__main__":
    main()
