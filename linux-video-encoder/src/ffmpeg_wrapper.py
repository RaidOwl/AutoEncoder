def run_ffmpeg_command(command):
    import subprocess

    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"FFmpeg command failed with error: {e.stderr.decode('utf-8')}") from e