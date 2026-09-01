"""Public Cloud Storage helper with resource retrieval primitives."""

from typing import Any, Dict, Optional

from google.cloud import storage

from ._cloud_storage_core import CloudStorage as _CloudStorageCore


class CloudStorage(_CloudStorageCore):
    """Extend the Cloud Storage helper with first-class object reads."""

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Any:
        """Return a pageable iterator of objects in a bucket.

        Parameters
        ----------
        bucket_name : str
            Name of the Cloud Storage bucket.
        prefix : str, optional
            Object-name prefix used to filter results.
        max_results : int, optional
            Maximum number of objects requested from the provider.
        page_token : str, optional
            Provider pagination token.

        Returns
        -------
        Any
            The Google Cloud pageable blob iterator.
        """
        arguments: Dict[str, Any] = {"prefix": prefix}
        if max_results is not None:
            arguments["max_results"] = max_results
        if page_token is not None:
            arguments["page_token"] = page_token
        return self._client.list_blobs(bucket_name, **arguments)

    def get_object(
        self, bucket_name: str, object_name: str
    ) -> Optional[storage.Blob]:
        """Return one Cloud Storage object when it exists.

        Parameters
        ----------
        bucket_name : str
            Name of the Cloud Storage bucket.
        object_name : str
            Name of the object within the bucket.

        Returns
        -------
        google.cloud.storage.Blob or None
            The object metadata handle, or ``None`` when it does not exist.
        """
        return self._client.bucket(bucket_name).get_blob(object_name)

    def get_object_metadata(
        self, bucket_name: str, object_name: str
    ) -> Dict[str, Any]:
        """Return JSON-compatible metadata for one object.

        Parameters
        ----------
        bucket_name : str
            Name of the Cloud Storage bucket.
        object_name : str
            Name of the object within the bucket.

        Returns
        -------
        dict[str, Any]
            Stable metadata fields for the requested object.

        Raises
        ------
        FileNotFoundError
            If the object does not exist.
        """
        blob = self.get_object(bucket_name, object_name)
        if blob is None:
            raise FileNotFoundError(
                f"Object not found: gs://{bucket_name}/{object_name}"
            )
        updated = getattr(blob, "updated", None)
        if updated is not None and hasattr(updated, "isoformat"):
            updated = updated.isoformat()
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "size": getattr(blob, "size", None),
            "content_type": getattr(blob, "content_type", None),
            "generation": getattr(blob, "generation", None),
            "updated": updated,
            "md5_hash": getattr(blob, "md5_hash", None),
        }

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """Return whether an object exists in a bucket.

        Parameters
        ----------
        bucket_name : str
            Name of the Cloud Storage bucket.
        object_name : str
            Name of the object within the bucket.

        Returns
        -------
        bool
            ``True`` when the object exists, otherwise ``False``.
        """
        return self.get_object(bucket_name, object_name) is not None


__all__ = ["CloudStorage"]
