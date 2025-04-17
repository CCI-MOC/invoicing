import tempfile
import shutil
from pathlib import Path
from unittest import TestCase


class BaseTestCaseWithTempDir(TestCase):
    def setUp(self):
        self.tempdir = Path(tempfile.TemporaryDirectory(delete=False).name)

    def tearDown(self):
        shutil.rmtree(self.tempdir)
