"""Public BigQuery helper with resource retrieval primitives."""

from typing import Any, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ._bigquery_core import DATA_TYPE_MAPPING, BigQuery as _BigQueryCore


class BigQuery(_BigQueryCore):
    """Extend the BigQuery helper with first-class resource reads."""

    def list_datasets(
        self,
        project: Optional[str] = None,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Any:
        """Return a pageable iterator of visible datasets.

        Parameters
        ----------
        project : str, optional
            Project whose datasets should be listed. The active client project
            is used when omitted.
        max_results : int, optional
            Maximum number of datasets requested from the provider.
        page_token : str, optional
            Provider pagination token.

        Returns
        -------
        Any
            The Google Cloud pageable dataset iterator.
        """
        arguments = {}
        if project is not None:
            arguments["project"] = project
        if max_results is not None:
            arguments["max_results"] = max_results
        if page_token is not None:
            arguments["page_token"] = page_token
        return self._client.list_datasets(**arguments)

    def get_dataset(self, dataset_id: str) -> bigquery.Dataset:
        """Return one BigQuery dataset.

        Parameters
        ----------
        dataset_id : str
            Dataset identifier accepted by the Google Cloud client.

        Returns
        -------
        google.cloud.bigquery.Dataset
            The requested dataset.
        """
        return self._client.get_dataset(dataset_id)

    def list_tables(
        self,
        dataset_id: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Any:
        """Return a pageable iterator of tables and views in a dataset.

        Parameters
        ----------
        dataset_id : str
            Dataset identifier accepted by the Google Cloud client.
        max_results : int, optional
            Maximum number of tables requested from the provider.
        page_token : str, optional
            Provider pagination token.

        Returns
        -------
        Any
            The Google Cloud pageable table iterator.
        """
        arguments = {}
        if max_results is not None:
            arguments["max_results"] = max_results
        if page_token is not None:
            arguments["page_token"] = page_token
        return self._client.list_tables(dataset_id, **arguments)

    def get_table(self, table_id: str) -> bigquery.Table:
        """Return one BigQuery table or view.

        Parameters
        ----------
        table_id : str
            Table identifier accepted by the Google Cloud client.

        Returns
        -------
        google.cloud.bigquery.Table
            The requested table or view.
        """
        return self._client.get_table(table_id)

    def get_table_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Return the schema fields for one table or view.

        Parameters
        ----------
        table_id : str
            Table identifier accepted by the Google Cloud client.

        Returns
        -------
        list[google.cloud.bigquery.SchemaField]
            A copy of the table schema fields.
        """
        return list(self.get_table(table_id).schema)

    def table_exists(self, table_id: str) -> bool:
        """Return whether a BigQuery table or view exists.

        Parameters
        ----------
        table_id : str
            Table identifier accepted by the Google Cloud client.

        Returns
        -------
        bool
            ``True`` when the resource exists, otherwise ``False``.
        """
        try:
            self.get_table(table_id)
            return True
        except NotFound:
            return False


__all__ = ["BigQuery", "DATA_TYPE_MAPPING"]
