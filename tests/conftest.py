import os
import pytest

# Ensure test mode flag is active for the test suite
os.environ["TASKFORGE_TESTING"] = "1"
os.environ["APP_ENV"] = "test"
