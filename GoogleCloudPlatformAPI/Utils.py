"""Generic utility helpers used across the package."""

import ast
import datetime
import json
import os
import pathlib
from glob import glob
from typing import Any, List, Optional, Tuple, Union


class FileHelper:
    """
    Utility functions for working with files.

    Methods
    -------
    save_to_json(obj, filepath)
        Serialise an object to a JSON file.
    read_json(filepath)
        Read a JSON file and return its contents.
    check_filepath(filepath)
        Ensure that the parent directory for ``filepath`` exists.
    split_filepath(fullfilepath)
        Split a full file path into path, name and extension.
    file_exists(fullfilepath)
        Return ``True`` if the file exists on disk.
    """

    @staticmethod
    def save_to_json(obj: Any, filepath: str) -> None:
        """
        Serialise an object to a JSON file.

        Parameters
        ----------
        obj : Any
            The object to serialise.
        filepath : str
            The path to the output JSON file.
        """

        def json_default(value):
            if isinstance(value, datetime.date):
                return dict(year=value.year, month=value.month, day=value.day)
            return value.__dict__

        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode="w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=4, default=json_default)

    @staticmethod
    def read_json(filepath: str) -> Any:
        """
        Read a JSON file and return its contents.

        Parameters
        ----------
        filepath : str
            The path to the JSON file.

        Returns
        -------
        Any
            The deserialized JSON content.
        """
        with open(file=filepath, mode="r") as json_file:
            return json.load(json_file)

    @staticmethod
    def check_filepath(filepath: str) -> None:
        """
        Ensure that the parent directory for ``filepath`` exists.

        If the directory does not exist, it will be created.

        Parameters
        ----------
        filepath : str
            The file path whose parent directory will be checked.
        """
        if (
            not os.path.exists(os.path.dirname(filepath))
            and len(os.path.dirname(filepath)) > 0
        ):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    @staticmethod
    def split_filepath(fullfilepath: str) -> Tuple[str, str, str]:
        """
        Split a full file path into path, name and extension.

        Parameters
        ----------
        fullfilepath : str
            The full path to the file.

        Returns
        -------
        tuple of str
            A tuple containing the file path, file name, and file extension.
        """
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
        """
        Return ``True`` if the file exists on disk.

        Parameters
        ----------
        fullfilepath : str
            The full path to the file.

        Returns
        -------
        bool
            ``True`` if the file exists, ``False`` otherwise.
        """
        file_path, file_name, file_extension = FileHelper.split_filepath(fullfilepath)

        files = glob(file_path + file_name + "-*" + file_extension)
        files.extend(glob(file_path + file_name + file_extension))
        return len(files) > 0


class ListHelper:
    """
    Functions that operate on lists.

    Methods
    -------
    chunk_list(lst, n)
        Yield ``n``-sized chunks from ``lst``.
    convert_list(val)
        Convert a string representation of a list to an actual list.
    merge_list(lst1, lst2=None)
        Merge two lists removing duplicates.
    """

    @staticmethod
    def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
        """
        Yield ``n``-sized chunks from ``lst``.

        Parameters
        ----------
        lst : list
            The list to chunk.
        n : int
            The size of each chunk.

        Returns
        -------
        list of list
            A list containing the chunks.
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val: Any) -> Any:
        """
        Convert a string representation of a list to an actual list.

        Parameters
        ----------
        val : Any
            The value to convert. If it's a string, it will be evaluated.

        Returns
        -------
        Any
            The converted list, or the original value if not a string.
        """
        if isinstance(val, str):
            try:
                return ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return val
        return val

    @staticmethod
    def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
        """
        Merge two lists removing duplicates.

        Parameters
        ----------
        lst1 : list
            The first list.
        lst2 : int or str or list, optional
            The second list, or a single value to append. Defaults to ``None``.

        Returns
        -------
        list
            The merged list with duplicates removed.
        """
        if isinstance(lst2, str) or isinstance(lst2, int):
            lst2 = [lst2]
        if lst2 is None:
            lst2 = []
        merged = list(lst1)
        merged.extend(lst2)
        return list(dict.fromkeys(merged))
