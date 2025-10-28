# autoencoder.py
"""
autoencoder.py

This script serves as the entry point for the video encoding application. It initializes the Scanner and (optionally) Encoder classes and orchestrates the process of finding and encoding video files.

Required Python version: >=3.7
Dependencies:
- HandBrakeCLI (installed on the system and available on PATH)
"""
from pathlib import Path
import json
import os
import time
import logging
import subprocess
import pathlib
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from scanner import Scanner
from encoder import Encoder  # kept as a fallback if needed

# locate config next to the project root
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.json"

DEFAULT_CONFIG = {
    "search_path": None,
    "output_dir": str(Path.home() / "Videos" / "encoded"),
    "min_size_mb": 100,
    "video_extensions": [".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"],
    "handbrake": {
        # HandBrakeCLI "quality" is RF (lower = higher quality), typical 18-23
        "encoder": "x264",
        "quality": 20,
        "audio_bitrate_kbps": 128,
        "extra_args": []  # list of additional args to append to HandBrakeCLI
    }
}

def load_config(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}
    # merge defaults (shallow merge is sufficient for this shape)
    merged = DEFAULT_CONFIG.copy()
    merged.update({k: v for k, v in cfg.items() if v is not None})
    # ensure output_dir is a string path
    merged["output_dir"] = str(Path(merged["output_dir"]))
    # ensure handbrake dict exists
    if "handbrake" not in merged or not isinstance(merged["handbrake"], dict):
        merged["handbrake"] = DEFAULT_CONFIG["handbrake"].copy()
    else:
        hb = DEFAULT_CONFIG["handbrake"].copy()
        hb.update({k: v for k, v in merged["handbrake"].items() if v is not None})
        merged["handbrake"] = hb
    return merged

import subprocess

def rip_disc(disc_index, output_dir_path, min_length=1800):
    """
    Rips a Blu-ray disc using MakeMKV CLI.
    Returns the path to the first output file if successful, or None on failure.
    """
    # return "/mnt/md0/ripped_discs/Interstellar_t00.mkv"
    logger = logging.getLogger(__name__)
    output_dir = output_dir_path.as_posix()
    cmd = [
        "makemkvcon", "mkv", f"disc:{disc_index}", "all", output_dir,
        f"--minlength={min_length}", "--progress=-same"
    ]

    # Check for existing MKVs
    existing_mkvs = sorted(output_dir_path.glob("*.mkv"))
    if existing_mkvs:
        latest = existing_mkvs[-1].resolve()
        print(f"⚠️  Found existing MKV file: {latest}\nSkipping rip.")
        return None

    print(f"📀 Running: {' '.join(cmd)}")

    try:
        result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        # Iterate lines as they arrive and log them
        if result.stdout is not None:
            for line in result.stdout:
                logger.info(line.rstrip())
        rc = result.wait()
        logger.debug("exited with code %s", rc)
    except FileNotFoundError:
        print("❌ Error: makemkvcon not found. Is MakeMKV installed?")
        return None

    # Print (or log) the MakeMKV output
    print(result.stdout)

    # Check for success
    if result.returncode != 0:
        print(f"❌ MakeMKV failed with code {result.returncode}.")
        return None

    print("✅ MakeMKV completed successfully.")

    # Look for any .mkv files in the output directory
    mkv_files = sorted(output_dir_path.glob("*.mkv"), key=os.path.getmtime)

    if not mkv_files:
        print("⚠️  No MKV files found in output directory.")
        return None

    # Return the most recent or first MKV file path
    first_file = str(mkv_files[-1].resolve())
    print(f"🎬 Output file: {first_file}")
    return first_file

def get_disc_number():
    """
    Returns the first detected MakeMKV disc index as an integer.
    If no disc is found, returns None.
    """
    try:
        # Query MakeMKV for available drives
        result = subprocess.run(
            ["makemkvcon", "-r", "info", "disc:9999"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print("Error running makemkvcon:", e.output.strip())
        return None

    output = result.stdout.strip()

    # Regex matches lines like:
    # DRV:0,0,999,0,"BD-RE HL-DT-ST BD-RE  WH16NS40 1.05","/dev/sr0"
    pattern = re.compile(r'DRV:(\d+),\d+,\d+,\d+,"([^"]+)","([^"]+)"')

    match = pattern.search(output)
    if match:
        disc_index = int(match.group(1))
        drive_name = match.group(2)
        device_path = match.group(3)
        print(f"Found disc in drive '{drive_name}' ({device_path}) — disc:{disc_index}")
        return disc_index

    print("No disc detected.")
    return None

def safe_move(src: Path, dst: Path) -> bool:
    src = Path(src)
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Fast path: same filesystem → atomic replace
        os.replace(src, dst)
        return True
    except OSError:
        pass

    # Cross-device: copy then atomically swap into place, then remove source
    try:
        tmp = dst.with_suffix(dst.suffix + ".part")
        shutil.copy2(src, tmp)     # preserves mtime/perm when possible
        os.replace(tmp, dst)       # atomic at destination
        os.unlink(src)             # remove original
        return True
    except Exception as e:
        logging.error("Failed to move %s to %s: %s", src, dst, e)
        return False

def run_encoder(input_path: str, output_path: str, opts: dict, ffmpeg: bool) -> bool:
    """
    Run HandBrakeCLI or ffmpeg and stream its stdout/stderr to the logger in real time.
    Returns True on success, False otherwise.
    """
    logger = logging.getLogger(__name__)
    encoder = opts.get("encoder", "x264")
    quality = opts.get("quality", "")
    width = opts.get("width", 1920)
    profile = opts.get("profile", "")
    height = opts.get("height", 1080)
    video = opts.get("video", "")
    audio = opts.get("audio", "")
    hwdev = opts.get("hwdev", "")
    filterdev = opts.get("filterdev", "")
    #audio_bitrate_kbps = opts.get("audio_bitrate_kbps", 128)
    extra = opts.get("extra_args", []) or []

    if ffmpeg:
        cmd = [
            "ffmpeg",
            "-y",  # overwrite output
            "-i", str(input_path),
            "-c:v", str(encoder),
            "-vf", str(video)
        ]
        if filterdev != "":
            cmd.insert(1, "-filter_hw_device")
            cmd.insert(2, str(filterdev))
        if hwdev != "":
            cmd.insert(1, "-init_hw_device")
            cmd.insert(2, str(hwdev))
        if quality != "":
            cmd.append("-global_quality")
            cmd.append(str(quality))
        if profile != "":
            cmd.append("-profile:v")
            cmd.append(str(profile))
        if audio != "":
            cmd.append("-c:a")
            cmd.append(str(audio))
        cmd.append(str(output_path))
        logger.info("Running ffmpeg: %s", " ".join(cmd))
    else:
        cmd = [
            "HandBrakeCLI",
            "-i", str(input_path),
            "-o", str(output_path),
            "-e", str(encoder),
            "-q", str(quality),
            "--width", str(width),
            "--height", str(height)
            #"-B", str(int(audio_bitrate_kbps))
        ]
        cmd.extend(map(str, extra))
        logger.info("Running HandBrakeCLI: %s", " ".join(cmd))
    try:
        # Stream combined stdout+stderr so progress and messages appear live
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        # Iterate lines as they arrive and log them
        if proc.stdout is not None:
            for line in proc.stdout:
                logger.info(line.rstrip())
        rc = proc.wait()
        logger.debug("exited with code %s", rc)
        return rc == 0
    except FileNotFoundError:
        logger.error("Encoder not found on PATH.")
        return False
    except KeyboardInterrupt:
        try:
            proc.kill()
        except Exception:
            pass
        logger.info("Encoding interrupted by user.")
        return False
    except Exception as e:
        logger.exception("Encoding run failed: %s", e)
        return False

def process_video(video_file: str, config: dict[str, any], output_dir: Path, rip_dir: Path, encoder: Encoder) -> bool:
    config_str = config.get("profile", "ffmpeg") 
    final_dir = config.get("final_dir", "")   
    # check if dvd, bluray, or video file    
    is_dvd = False
    is_bluray = False
    if any(s in video_file.lower() for s in ["bdmv", "bluray"]):
        is_bluray = True
        config_str = "handbrake_br"
    elif any(s in video_file.lower() for s in ["video_ts"]):
        is_dvd = True 
        config_str = "handbrake_dvd"

    hb_opts = config.get(config_str, {})
    extension = hb_opts.get("extension", ".mp4")

    src = Path(video_file)
    src_mtime = src.stat().st_mtime
    if is_dvd:
        out_name = f"{src.parent.name}{extension}"
    elif is_bluray:
        out_name = f"{src.parent.parent.name}{extension}"
    else:
        out_name = f"{src.stem}_encoded_{src_mtime}{extension}" # this ensures re-encoding if the source file has the same name but is newer
    out_path = output_dir / out_name

    # skip if output already exists
    if out_path.exists():
        try:
            logging.info("Skipping already-encoded file: %s", out_path)
            return False
        except Exception:
            logging.debug("Failed to stat files %s or %s; proceeding to encode", src, out_path)

    # if its a bluray then rip it first
    if is_bluray:
        logging.info("Ripping Blu-ray disc from %s", video_file)
        disc_num = get_disc_number()
        if disc_num is not None:
            rip_path = rip_disc(disc_num, rip_dir)
            if rip_path is None:
                logging.error("Blu-ray ripping failed; skipping encoding for %s", video_file)
                return False
        else:
            logging.error("No Blu-ray disc detected; skipping encoding for %s", video_file)
            return False

        video_file = rip_path  # use ripped path for encoding
    if video_file is None:
        return False
    # prefer HandBrakeCLI; if it fails, fall back to encoder.encode_video if available                         
    success = run_encoder(video_file, str(out_path), hb_opts, not (is_dvd or is_bluray))
    if not success:
        logging.warning("Encoding failed for %s -> %s; attempting Software Encoder fallback", video_file, out_path)
        try:
            ffmpeg_opts = config.get("ffmpeg_fallback", {})
            try:
                encoder.encode_video(video_file, str(out_path), ffmpeg_opts)
            except TypeError:
                encoder.encode_video(video_file, str(out_path))
            logging.info("Fallback encoder succeeded: %s -> %s", video_file, out_path)
        except Exception:
            logging.exception("Fallback encoder failed for %s", video_file)
            return False
    else:
        logging.info("Encoded %s -> %s (HandBrakeCLI)", video_file, out_path)
        # move final file to final_dir if specified
        if final_dir != "":
            try:
                final_path = Path(final_dir) / out_name
                out = safe_move(out_path, final_path)
                if out:
                    # create a blank file at the original location to indicate completion
                    out_path.touch()
                    logging.info("Moved encoded file to final directory: %s", final_path)
            except Exception:
                logging.debug("Failed to move encoded file to final directory: %s", final_dir, exc_info=True)
        if is_bluray:
            try:
                # delete ripped file to save space
                rip_fp = Path(video_file)
                rip_fp.unlink()
                logging.info("Deleted ripped Blu-ray file: %s", rip_fp)
            except Exception:
                logging.debug("Failed to delete ripped Blu-ray file: %s", video_file, exc_info=True)

def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s: %(message)s")
    config = load_config(CONFIG_PATH)
    max_threads = config.get("max_threads", 4)
    search_path = config.get("search_path")  # None => auto-detect plugged drives
    # treat explicit "/" as "auto-detect" (don't scan the system root)
    if search_path == '/':
        search_path = None
    output_dir = Path(config.get("output_dir"))
    output_dir.mkdir(parents=True, exist_ok=True)
    rip_dir = Path(config.get("rip_dir"))
    rip_dir.mkdir(parents=True, exist_ok=True)

    # initialize scanner:
    # - if config provides an explicit search_path, scan only that path
    # - otherwise let Scanner autodetect candidate mountpoints for plugged drives
    if search_path:
        scanner = Scanner(search_path=search_path)
    else:
        scanner = Scanner()  # Scanner default uses candidate mounts when search_path == '/'

    # try to pass config into Encoder if its constructor accepts it,
    # otherwise fall back to plain construction
    try:
        encoder = Encoder(config=config)
    except TypeError:
        encoder = Encoder()

    rescan_interval = float(config.get("rescan_interval", 30))

    logging.info("Starting continuous scanner. search_path=%s output=%s interval=%.1fs",
                 search_path if search_path else "<auto-detect>", output_dir, rescan_interval)

    try:
        while True:
            # decide which directories will be scanned this pass
            if search_path:
                scan_roots = [search_path]
            else:
                try:
                    # ensure devices are mounted and get mountpoints to scan
                    scan_roots = scanner.ensure_mounted_candidates()
                except Exception:
                    scan_roots = []

            logging.info("Scanning directories: %s", ", ".join(scan_roots) if scan_roots else "<none>")
            # debug: show mount status and a sample of contents for each scan root
            import os
            for root in scan_roots:
                try:
                    is_m = os.path.ismount(root)
                    entries = []
                    if os.path.exists(root):
                        try:
                            entries = os.listdir(root)[:10]
                        except Exception:
                            entries = ["<unreadable>"]
                    logging.debug("Scan root: %s ismount=%s exists=%s sample=%s", root, is_m, os.path.exists(root), entries)
                except Exception:
                    logging.debug("Scan root: %s inspect failed", root)

            video_files = scanner.find_video_files(scan_roots)
            if not video_files:
                logging.debug("No candidate video files found on this pass.")
            #for video_file in video_files:               
            with ThreadPoolExecutor(max_workers=max_threads) as ex:
                futures = {ex.submit(process_video, f, config, output_dir, rip_dir, encoder): f for f in video_files}
                for fut in as_completed(futures):
                    src = futures[fut]
                    try:
                        out = fut.result()
                        print(f"✅ {src} → {out}")
                    except Exception as e:
                        print(f"❌ {src}: {e}") 

            time.sleep(rescan_interval)
            # after a successful scan/pass, unmount devices the scanner mounted
            try:
                unmounted = scanner.unmount_mountpoints()
                if unmounted:
                    logging.info("Automatically unmounted: %s", ", ".join(unmounted))
            except Exception:
                logging.debug("Automatic unmount step failed", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Interrupted, shutting down.")
    except Exception:
        logging.exception("Unexpected error in main loop.")
    finally:
        logging.info("Exited.")

if __name__ == "__main__":
    main()