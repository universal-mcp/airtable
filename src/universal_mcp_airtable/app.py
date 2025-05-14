from typing import Any, Dict, List, Iterable
from universal_mcp.applications import APIApplication
from universal_mcp.integrations import Integration

# Import necessary components from pyairtable
from pyairtable import Api
from pyairtable.api.base import Base
from pyairtable.api.table import Table
from pyairtable.api.types import (
    RecordDict, RecordId, WritableFields,
    RecordDeletedDict, UpdateRecordDict, UpsertResultDict
)
from pyairtable.formulas import Formula, to_formula_str

class AirtableApp(APIApplication):
    """
    Application for interacting with the Airtable API to manage bases, tables,
    and records. Requires an Airtable API key configured via integration.
    """

    def __init__(self, integration: Integration | None = None) -> None:
        super().__init__(name="airtable", integration=integration)

    def _get_client(self) -> Api:
        """Initializes and returns the pyairtable client after ensuring API key is set."""
        if not self.integration:
             raise ValueError("Integration is not set for AirtableApp.")
        credentials = self.integration.get_credentials()
        api_key = credentials.get("api_key") or credentials.get("apiKey") or credentials.get("API_KEY")
        return Api(api_key)

    def list_bases(self) -> List[Base] | str:
        """
        Lists all bases accessible with the current API key.

        Returns:
            A list of pyairtable.api.base.Base objects on success,
            or a string containing an error message on failure.

        Tags:
            list, base, important
        """
        try:
            client = self._get_client()
            # The bases() method returns Base objects, which are Pydantic models.
            # Returning these provides richer information than just IDs/names.
            return client.bases()
        except Exception as e:
            return f"Error listing bases: {type(e).__name__} - {e}"

    def list_tables(self, base_id: str) -> List[Table] | str:
        """
        Lists all tables within a specified base.

        Args:
            base_id: The ID of the base.

        Returns:
            A list of pyairtable.api.table.Table objects on success,
            or a string containing an error message on failure.

        Tags:
            list, table, important
        """
        try:
            client = self._get_client()
            base = client.base(base_id)
            # The tables() method returns Table objects, which are Pydantic models.
            return base.tables()
        except Exception as e:
            return f"Error listing tables for base '{base_id}': {type(e).__name__} - {e}"

    def get_record(
        self,
        base_id: str,
        table_id_or_name: str,
        record_id: RecordId,
        **options: Any
    ) -> RecordDict | str:
        """
        Retrieves a single record by its ID from a specified table within a base.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            record_id: The ID of the record to retrieve.
            **options: Additional options for retrieving the record (e.g., cell_format, user_locale).

        Returns:
            A dictionary representing the record on success,
            or a string containing an error message on failure.

        Tags:
            get, record, important
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.get(record_id, **options)
        except Exception as e:
            return f"Error getting record '{record_id}' from '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def list_records(
        self,
        base_id: str,
        table_id_or_name: str,
        **options: Any
    ) -> List[RecordDict] | str:
        """
        Lists records from a specified table within a base.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            **options: Additional options for listing records (e.g., view, max_records, formula, sort).
                       Formula can be a string or a pyairtable.formulas.Formula object.

        Returns:
            A list of dictionaries, where each dictionary represents a record, on success,
            or a string containing an error message on failure.

        Tags:
            list, record, important
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)

            # Convert Formula object to string if provided
            if 'formula' in options and isinstance(options['formula'], Formula):
                 options['formula'] = to_formula_str(options['formula'])

            return table.all(**options)
        except Exception as e:
            return f"Error listing records from '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def create_record(
        self,
        base_id: str,
        table_id_or_name: str,
        fields: WritableFields,
        **options: Any
    ) -> RecordDict | str:
        """
        Creates a new record in a specified table within a base.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            fields: A dictionary where keys are field names/IDs and values are the field data.
            **options: Additional options for creating the record (e.g., typecast).

        Returns:
            A dictionary representing the newly created record on success,
            or a string containing an error message on failure.

        Tags:
            create, record, important
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.create(fields=fields, **options)
        except Exception as e:
            return f"Error creating record in '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def update_record(
        self,
        base_id: str,
        table_id_or_name: str,
        record_id: RecordId,
        fields: WritableFields,
        **options: Any
    ) -> RecordDict | str:
        """
        Updates an existing record in a specified table within a base.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            record_id: The ID of the record to update.
            fields: A dictionary where keys are field names/IDs and values are the field data to update.
            **options: Additional options for updating the record (e.g., typecast, replace).

        Returns:
            A dictionary representing the updated record on success,
            or a string containing an error message on failure.

        Tags:
            update, record
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.update(record_id, fields=fields, **options)
        except Exception as e:
            return f"Error updating record '{record_id}' in '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def delete_record(
        self,
        base_id: str,
        table_id_or_name: str,
        record_id: RecordId
    ) -> RecordDeletedDict | str:
        """
        Deletes a record from a specified table within a base.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            record_id: The ID of the record to delete.

        Returns:
            A dictionary confirming the deletion on success,
            or a string containing an error message on failure.

        Tags:
            delete, record
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.delete(record_id)
        except Exception as e:
            return f"Error deleting record '{record_id}' from '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def batch_create_records(
        self,
        base_id: str,
        table_id_or_name: str,
        records: Iterable[WritableFields],
        **options: Any
    ) -> List[RecordDict] | str:
        """
        Creates multiple records in batches in a specified table.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            records: An iterable of dictionaries, where each dictionary contains the fields for a new record.
            **options: Additional options for creating records (e.g., typecast).

        Returns:
            A list of dictionaries representing the newly created records on success,
            or a string containing an error message on failure.

        Tags:
            create, record, batch
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.batch_create(records, **options)
        except Exception as e:
            return f"Error batch creating records in '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def batch_update_records(
        self,
        base_id: str,
        table_id_or_name: str,
        records: Iterable[UpdateRecordDict],
        **options: Any
    ) -> List[RecordDict] | str:
        """
        Updates multiple records in batches in a specified table.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            records: An iterable of dictionaries, where each dictionary must contain 'id' and 'fields' for the record to update.
            **options: Additional options for updating records (e.g., typecast, replace).

        Returns:
            A list of dictionaries representing the updated records on success,
            or a string containing an error message on failure.

        Tags:
            update, record, batch
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.batch_update(records, **options)
        except Exception as e:
            return f"Error batch updating records in '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def batch_delete_records(
        self,
        base_id: str,
        table_id_or_name: str,
        record_ids: Iterable[RecordId]
    ) -> List[RecordDeletedDict] | str:
        """
        Deletes multiple records in batches from a specified table.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            record_ids: An iterable of record IDs to delete.

        Returns:
            A list of dictionaries confirming the deletion status for each record on success,
            or a string containing an error message on failure.

        Tags:
            delete, record, batch
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.batch_delete(record_ids)
        except Exception as e:
            return f"Error batch deleting records from '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def batch_upsert_records(
        self,
        base_id: str,
        table_id_or_name: str,
        records: Iterable[Dict[str, Any]],
        key_fields: List[str],
        **options: Any
    ) -> UpsertResultDict | str:
        """
        Updates or creates records in batches in a specified table.

        Records are matched by 'id' if provided, or by 'key_fields'.

        Args:
            base_id: The ID of the base.
            table_id_or_name: The ID or name of the table.
            records: An iterable of dictionaries, where each dictionary contains either
                     'id' and 'fields' for existing records, or just 'fields' for new records.
            key_fields: A list of field names/IDs used to match records if 'id' is not provided.
            **options: Additional options for upserting records (e.g., typecast, replace).

        Returns:
            A dictionary containing lists of created/updated record IDs and the affected records on success,
            or a string containing an error message on failure.

        Tags:
            create, update, record, batch, upsert
        """
        try:
            client = self._get_client()
            table = client.table(base_id, table_id_or_name)
            return table.batch_upsert(records, key_fields=key_fields, **options)
        except Exception as e:
            return f"Error batch upserting records in '{table_id_or_name}' in '{base_id}': {type(e).__name__} - {e}"

    def list_tools(self):
        """Returns a list of methods exposed as tools."""
        return [
            self.list_bases,
            self.list_tables,
            self.get_record,
            self.list_records,
            self.create_record,
            self.update_record,
            self.delete_record,
            self.batch_create_records,
            self.batch_update_records,
            self.batch_delete_records,
            self.batch_upsert_records,
        ]
