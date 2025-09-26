"""Generic utility helpers used across the package.

This module contains helper classes for common file and list operations.

Public Classes
--------------
- FileHelper: Utility functions for working with files.
- ListHelper: Functions that operate on lists.
"""

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

        Raises
        ------
        IOError
            If the file cannot be written to.
        TypeError
            If the object contains a type that cannot be serialized to JSON.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import FileHelper

        my_dict = {"key": "value", "date": datetime.date(2023, 1, 1)}
        FileHelper.save_to_json(my_dict, "/tmp/my_data.json")
        ```
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

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        json.JSONDecodeError
            If the file is not valid JSON.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import FileHelper

        # Assuming /tmp/my_data.json exists from the previous example
        data = FileHelper.read_json("/tmp/my_data.json")
        print(data['key'])
        # value
        ```
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

        Raises
        ------
        OSError
            If the directory cannot be created.
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
        tuple[str, str, str]
            A tuple containing the file path, file name, and file extension.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import FileHelper

        path, name, ext = FileHelper.split_filepath("/path/to/my_file.txt.gz")
        print(path)
        # /path/to/
        print(name)
        # my_file
        print(ext)
        # .txt.gz
        ```
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

        This method supports wildcards in the filename part.

        Parameters
        ----------
        fullfilepath : str
            The full path to the file, which can include wildcards.

        Returns
        -------
        bool
            ``True`` if a matching file exists, ``False`` otherwise.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import FileHelper

        # Create a dummy file
        with open("/tmp/my_file-2023.txt", "w") as f:
            f.write("test")

        exists = FileHelper.file_exists("/tmp/my_file-*.txt")
        print(exists)
        # True
        ```
        """
        file_path, file_name, file_extension = FileHelper.split_filepath(fullfilepath)

        files = glob(file_path + file_name + "-*" + file_extension)
        files.extend(glob(file_path + file_name + file_extension))
        return len(files) > 0


class ListHelper:
    """
    Functions that operate on lists.
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
        list[list[Any]]
            A list containing the chunks.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import ListHelper

        my_list = [1, 2, 3, 4, 5, 6, 7]
        chunks = ListHelper.chunk_list(my_list, 3)
        print(chunks)
        # [[1, 2, 3], [4, 5, 6], [7]]
        ```
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val: Any) -> Any:
        """
        Convert a string representation of a list to an actual list.

        If the value is not a string or cannot be parsed as a list, it is
        returned as is.

        Parameters
        ----------
        val : Any
            The value to convert. If it's a string, it will be evaluated.

        Returns
        -------
        Any
            The converted list, or the original value if not a string or on error.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import ListHelper

        str_list = "[1, 'a', 3.0]"
        py_list = ListHelper.convert_list(str_list)
        print(isinstance(py_list, list))
        # True
        print(py_list[1])
        # 'a'
        ```
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
            The merged list with duplicates removed, preserving order.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Utils import ListHelper

        list1 = [1, 2, 3]
        list2 = [3, 4, 5]
        merged = ListHelper.merge_list(list1, list2)
        print(merged)
        # [1, 2, 3, 4, 5]

        merged_single = ListHelper.merge_list(list1, 4)
        print(merged_single)
        # [1, 2, 3, 4]
        ```
        """
        if isinstance(lst2, (str, int)):
            lst2 = [lst2]
        if lst2 is None:
            lst2 = []
        merged = list(lst1)
        merged.extend(lst2)
        return list(dict.fromkeys(merged))