# Linux Video Encoder

This project provides a Python-based solution for finding and encoding video files on a Linux machine using FFmpeg and HandBrakeCLI. It consists of several modules that work together to scan for video files, encode them, and provide a user-friendly interface for operation.


## Installation

To install the necessary dependencies, run:

```
sudo apt-get install -y python3 ffmpeg libdvdread4 libbluray-bdj libdvdcss2 udisks2
pip install -r requirements.txt
```

If you are working with blurays you'll need 'makemkv'. Depending on your OS you will have different [installation methods](https://makemkv.com/downloads)


## Usage

To find and encode video files, you can run the script using python

```
sudo python3 path/to/your/directory/autoencoder.py
```

This will initiate the scanning of connected video files and encode them using the specified settings in the `config.json` file.

## Config

* __search_path__ - You can specify a specific directory to search if the scan doesn't find your files.
* __output_dir__ - Where your videos will be encoded to
* __rip_dir__ - Where your ripped blurays will be saved
* __final_dir__ - Where you encoded video are sent after a sucessful run. Set to null if the output_dir if where you want them to stay.
* __max_threads__ - How many simulaneous encodes you want running
* __rescan_interval__ - Wait time between scans in seconds
* __min_size_mb__ - The minimum size in Megabytes for a video to be encoded
* __video_extensions__ - A list of video extensions that will be encoded
* __profile__ - The profile you want to use for encoding
* __profiles__ - These are examples created that use the ffmpeg and HandBrakeCLI commands. See their docs for other parameters.


## License

This project is licensed under the MIT License. See the LICENSE file for more details.