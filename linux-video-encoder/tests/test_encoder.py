import unittest
from src.encoder import Encoder

class TestEncoder(unittest.TestCase):

    def setUp(self):
        self.encoder = Encoder()

    def test_encode_video(self):
        input_path = "test_video.mp4"
        output_path = "encoded_video.mp4"
        result = self.encoder.encode_video(input_path, output_path)
        self.assertTrue(result)  # Assuming encode_video returns True on success

    def test_encode_video_invalid_path(self):
        input_path = "invalid_video.mp4"
        output_path = "encoded_video.mp4"
        with self.assertRaises(FileNotFoundError):
            self.encoder.encode_video(input_path, output_path)

if __name__ == '__main__':
    unittest.main()