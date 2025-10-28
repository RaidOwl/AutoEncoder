class Encoder:
    def __init__(self):
        pass

    def encode_video(self, input_path, output_path):
        """
        Encodes the video file located at input_path and saves it to output_path.
        
        Parameters:
            input_path (str): The path to the input video file.
            output_path (str): The path where the encoded video will be saved.
        """
        import subprocess

        command = [
            'ffmpeg',
            '-i', input_path,
            '-c:v', 'libx264',  # Example codec
            '-preset', 'fast',   # Example preset
            output_path
        ]

        try:
            subprocess.run(command, check=True)
            print(f"Successfully encoded {input_path} to {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error encoding video: {e}")