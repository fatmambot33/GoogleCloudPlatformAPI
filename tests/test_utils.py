import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.Utils import FileHelper, ListHelper

def test_split_filepath(tmp_path):
    file_path = tmp_path / 'data' / 'file.csv.gz'
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch()
    path, name, ext = FileHelper.split_filepath(str(file_path))
    assert path.endswith('data/')
    assert name == 'file'
    assert ext == '.csv.gz'

def test_merge_list_with_scalar():
    assert ListHelper.merge_list([1, 2], 3) == [1, 2, 3]


def test_merge_list_with_list():
    assert ListHelper.merge_list([1], [1, 2]) == [1, 2]
