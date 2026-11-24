import os
import sys
import tempfile
from pathlib import Path

# Importing the engine must never initialize the desktop data directory.
_test_data = tempfile.TemporaryDirectory(prefix="netease-test-")
os.environ["NETEASE_DATA_DIR"] = _test_data.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
