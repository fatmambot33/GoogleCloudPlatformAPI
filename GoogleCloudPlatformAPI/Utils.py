"""Generic utility helpers used across the package."""

import ast
import datetime
import json
import os
import pathlib
from glob import glob
from typing import List, Optional, Union


class FileHelper:
    """Utility functions for working with files."""

    @staticmethod
    def save_to_json(obj, filepath: str) -> None:
        """Serialise an object to a JSON file."""

        def json_default(value):
            if isinstance(value, datetime.date):
                return dict(year=value.year, month=value.month, day=value.day)
            return value.__dict__

        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode="w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=4, default=json_default)

    @staticmethod
    def read_json(filepath: str):
        """Read a JSON file and return its contents."""
        with open(file=filepath, mode="r") as json_file:
            return json.load(json_file)

    @staticmethod
    def check_filepath(filepath: str) -> None:
        """Ensure that the parent directory for ``filepath`` exists."""
        if not os.path.exists(os.path.dirname(filepath)) and len(os.path.dirname(filepath)) > 0:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    @staticmethod
    def split_filepath(fullfilepath: str):
        """Split a full file path into path, name and extension."""
        p = pathlib.Path(fullfilepath)
        file_path = str(p.parent) + "/"
        file_name = p.name
        file_extension = ""
        for suffix in p.suffixes:
            file_name = file_name.replace(suffix, "")
            file_extension = file_extension + suffix
        return file_path, file_name, file_extension

    @staticmethod
    def file_exists(fullfilepath: str) -> bool:
        """Return ``True`` if the file exists on disk."""
        file_path, file_name, file_extension = FileHelper.split_filepath(fullfilepath)

        if len(glob(file_path + file_name + "-*" + file_extension) + glob(file_path + file_name + file_extension)) == 0:
            return False
        return True


class ListHelper:
    """Functions that operate on lists."""

    @staticmethod
    def chunk_list(lst: List, n: int) -> List[List]:
        """Yield ``n``-sized chunks from ``lst``."""
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val):
        """Convert a string representation of a list to an actual list."""
        if isinstance(val, str):
            return ast.literal_eval(val)
        return val

    @staticmethod
    def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
        """Merge two lists removing duplicates."""
        if isinstance(lst2, str) or isinstance(lst2, int):
            lst2 = [lst2]
        if lst2 is None:
            lst2 = []
        return list(dict.fromkeys(lst1 + lst2))
