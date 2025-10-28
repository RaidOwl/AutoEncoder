# Usage Instructions for Linux Video Encoder

## Overview
The Linux Video Encoder is a Python-based tool that scans your Linux machine for connected video files and encodes them using FFmpeg. This document provides instructions on how to install the necessary dependencies and use the encoder.

## Installation

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd linux-video-encoder
   ```

2. **Install Dependencies**
   Ensure you have Python 3.6 or higher installed. You can install the required Python packages using pip:
   ```bash
   pip install -r requirements.txt
   ```

3. **Install FFmpeg**
   You need to have FFmpeg installed on your system. You can install it using your package manager. For example, on Ubuntu, you can run:
   ```bash
   sudo apt update
   sudo apt install ffmpeg
   ```

## Usage

1. **Run the Encoder Script**
   You can run the encoder script directly from the command line. Use the provided shell script to execute the encoder:
   ```bash
   ./scripts/run_encoder.sh
   ```

2. **Encoding Process**
   The script will automatically scan for video files connected to your machine and encode them. The default encoding settings can be modified in the `encoder.py` file.

3. **Output**
   The encoded video files will be saved in the same directory as the original files, with a modified file extension (e.g., `.mp4`).

## Examples

- To encode a specific video file, you can modify the `autoencoder.py` file to specify the input and output paths directly.

## Troubleshooting
- If you encounter any issues, ensure that FFmpeg is correctly installed and accessible from your command line.
- Check the logs for any error messages that may indicate what went wrong during the encoding process.

## Contributing
Feel free to submit issues or pull requests if you would like to contribute to the project.