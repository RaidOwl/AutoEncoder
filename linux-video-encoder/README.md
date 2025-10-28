# Linux Video Encoder

This project provides a Python-based solution for finding and encoding video files on a Linux machine using FFmpeg. It consists of several modules that work together to scan for video files, encode them, and provide a user-friendly interface for operation.

## Project Structure

```
linux-video-encoder
├── src
│   ├── encoder.py          # Main encoding logic using FFmpeg
│   ├── scanner.py          # Scans for video files on the system
│   ├── ffmpeg_wrapper.py    # Wrapper for FFmpeg command execution
│   ├── autoencoder.py      # Entry point for the application
│   └── __init__.py        # Package initialization
├── tests
│   ├── test_scanner.py     # Unit tests for the Scanner class
│   ├── test_encoder.py      # Unit tests for the Encoder class
│   └── __init__.py        # Package initialization for tests
├── scripts
│   └── run_encoder.sh      # Shell script to run the encoder
├── docs
│   └── usage.md           # Documentation on usage and installation
├── pyproject.toml         # Project metadata and dependencies
├── requirements.txt       # Python dependencies
├── .gitignore             # Files to ignore in Git
├── LICENSE                # Licensing information
└── README.md              # Project overview and documentation
```

## Installation

To install the necessary dependencies, run:

```
pip install -r requirements.txt
```

Make sure you have FFmpeg installed on your system. You can install it using your package manager, for example:

```
sudo apt-get install ffmpeg
```

## Usage

To find and encode video files, you can run the encoder script using the provided shell script:

```
bash scripts/run_encoder.sh
```

This will initiate the scanning of connected video files and encode them using the specified settings in the `encoder.py` module.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.