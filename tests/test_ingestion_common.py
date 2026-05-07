import hashlib
import math
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "ingestion"))

from common import deterministic_batch_id, file_checksum, stable_hash  # noqa: E402


def test_file_checksum_is_md5_of_file_contents(tmp_path):
    file_path = tmp_path / "sample.csv"
    file_path.write_text("id,name\n1,Ada\n", encoding="utf-8")

    expected = hashlib.md5(b"id,name\n1,Ada\n").hexdigest()

    assert file_checksum(file_path) == expected


def test_deterministic_batch_id_is_stable_for_same_input():
    batch_id = deterministic_batch_id("jobs", "jobs.csv", "abc123")

    assert batch_id == deterministic_batch_id("jobs", "jobs.csv", "abc123")


def test_deterministic_batch_id_changes_when_checksum_changes():
    first = deterministic_batch_id("jobs", "jobs.csv", "abc123")
    second = deterministic_batch_id("jobs", "jobs.csv", "def456")

    assert first != second


def test_stable_hash_normalizes_null_like_values():
    left = stable_hash(["candidate-1", None, "Python"])
    right = stable_hash(["candidate-1", math.nan, "Python"])

    assert left == right


def test_stable_hash_changes_when_business_value_changes():
    first = stable_hash(["candidate-1", "python"])
    second = stable_hash(["candidate-1", "sql"])

    assert first != second
