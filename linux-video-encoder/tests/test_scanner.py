import unittest
from src.scanner import Scanner

class TestScanner(unittest.TestCase):

    def setUp(self):
        self.scanner = Scanner()

    def test_find_video_files(self):
        # Assuming there are some video files in a known directory for testing
        video_files = self.scanner.find_video_files()
        self.assertIsInstance(video_files, list)
        # Add more assertions based on expected video files

if __name__ == '__main__':
    unittest.main()