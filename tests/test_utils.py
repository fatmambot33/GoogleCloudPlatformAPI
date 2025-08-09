import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.Utils import FileHelper, ListHelper


def test_split_filepath(tmp_path: Path):
    """Test the split_filepath method."""
    file_path = tmp_path / "data" / "file.csv.gz"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch()
    path, name, ext = FileHelper.split_filepath(str(file_path))
    assert path.endswith("data/")
    assert name == "file"
    assert ext == ".csv.gz"


def test_merge_list_with_scalar():
    """Test the merge_list method with a scalar value."""
    assert ListHelper.merge_list([1, 2], 3) == [1, 2, 3]


def test_merge_list_with_list():
    """Test the merge_list method with another list."""
    assert ListHelper.merge_list([1], [1, 2]) == [1, 2]


def test_save_and_read_json(tmp_path: Path):
    """Test saving to and reading from a JSON file."""
    data = {"a": 1, "b": [1, 2, 3]}
    file_path = tmp_path / "test.json"
    FileHelper.save_to_json(data, str(file_path))
    assert file_path.exists()
    read_data = FileHelper.read_json(str(file_path))
    assert read_data == data


def test_check_filepath(tmp_path: Path):
    """Test that the check_filepath method creates a directory."""
    dir_path = tmp_path / "new_dir"
    file_path = dir_path / "test.txt"
    assert not dir_path.exists()
    FileHelper.check_filepath(str(file_path))
    assert dir_path.exists()


def test_file_exists(tmp_path: Path):
    """Test the file_exists method."""
    file_path = tmp_path / "existing_file.txt"
    file_path.touch()
    assert FileHelper.file_exists(str(file_path)) is True
    assert FileHelper.file_exists(str(tmp_path / "non_existing_file.txt")) is False


def test_chunk_list():
    """Test the chunk_list method."""
    my_list = [1, 2, 3, 4, 5, 6, 7]
    assert ListHelper.chunk_list(my_list, 3) == [[1, 2, 3], [4, 5, 6], [7]]
    assert ListHelper.chunk_list(my_list, 7) == [[1, 2, 3, 4, 5, 6, 7]]
    assert ListHelper.chunk_list(my_list, 10) == [[1, 2, 3, 4, 5, 6, 7]]
    assert ListHelper.chunk_list([], 3) == []


def test_convert_list():
    """Test the convert_list method."""
    assert ListHelper.convert_list("[1, 2, 3]") == [1, 2, 3]
    assert ListHelper.convert_list("['a', 'b']") == ["a", "b"]
    assert ListHelper.convert_list([1, 2, 3]) == [1, 2, 3]
    assert ListHelper.convert_list("not a list") == "not a list"
