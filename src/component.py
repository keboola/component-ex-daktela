"""
Daktela Extractor Component for Keboola.

Extracts data from Daktela CRM/Contact Center system via API v6.
"""

import asyncio
import csv
import logging
import sys
import time
from pathlib import Path
from typing import Any, Optional

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from configuration import Configuration
from daktela_client import DaktelaClient
from data_transformer import DataTransformer
from table_config import TableConfig, get_table_config

class Component(ComponentBase):
    """
    Daktela Extractor Component.

    Extracts data from Daktela API v6 and produces CSV output.
    """

    SPECIAL_TABLES: set[str] = {"activities", "activities_statuses"}
    TABLES_WITH_REQUIREMENTS: set[str] = {
        "activities_call", "activities_email", "activities_chat"
    }

    def __init__(self):
        super().__init__()
        self.config: Optional[Configuration] = None
        self.server_name: str = ""
        self.extracted_parent_ids: dict[str, dict[str, set[str]]] = {}
        self.invalid_activities: list[str] = []

    def run(self) -> None:
        """Main execution code."""
        start_time = time.time()

        self._init_configuration()
        self._validate_configuration()

        # Load state for incremental processing
        state = self.get_state_file()
        logging.info(f"Loaded state: {state}")

        asyncio.run(self._extract_all_tables())

        # Save state for next run
        new_state = {
            "last_run": time.strftime("%Y-%m-%d %H:%M:%S"),
            "tables_extracted": self.config.get_table_list()
        }
        self.write_state_file(new_state)
        logging.info(f"Saved state: {new_state}")

        elapsed = time.time() - start_time
        logging.info(f"Elapsed time of extraction: {elapsed:.2f} seconds.")

    def _init_configuration(self) -> None:
        """Initialize configuration from parameters."""
        self.config = Configuration(**self.configuration.parameters)
        self.server_name = self.config.get_server_name()

        if self.config.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("Debug mode enabled")

    def _validate_configuration(self) -> None:
        """Validate configuration parameters."""
        self.config.validate_date_range()

        requested_tables = self.config.get_table_list()
        for table_name in requested_tables:
            table_config = get_table_config(table_name)
            if not table_config:
                logging.warning(f"Table '{table_name}' is not configured. Skipping.")

    async def _extract_all_tables(self) -> None:
        """Extract all requested tables."""
        requested_tables = self.config.get_table_list()

        tables_without_requirements = []
        tables_with_requirements = []

        for table_name in requested_tables:
            table_config = get_table_config(table_name)
            if not table_config:
                continue

            if table_config.has_requirements() or table_name in self.SPECIAL_TABLES:
                tables_with_requirements.append(table_name)
            else:
                tables_without_requirements.append(table_name)

        async with DaktelaClient(
            self.config.get_base_url(),
            self.config.username,
            self.config.password
        ) as client:
            # Extract independent tables
            for table_name in tables_without_requirements:
                await self._extract_table(client, table_name)

            # Extract special tables (activities, activities_statuses)
            special_tables_to_extract = [
                t for t in ["activities", "activities_statuses"]
                if t in tables_with_requirements
            ]
            for table_name in special_tables_to_extract:
                await self._extract_table(client, table_name)

            # Extract child tables that depend on parent tables
            remaining_tables = [
                t for t in tables_with_requirements
                if t not in self.SPECIAL_TABLES
            ]
            for table_name in remaining_tables:
                await self._extract_child_table(client, table_name)

    async def _extract_table(self, client: DaktelaClient, table_name: str) -> None:
        """Extract a standard table."""
        table_config = get_table_config(table_name)
        if not table_config:
            return

        start_time = time.time()

        filters = self._prepare_filters(table_config)
        api_table_name = table_config.get_api_table_name()

        data = await client.extract_table(
            api_table_name,
            filters=filters,
            fields=table_config.columns if table_config.columns else None
        )

        transformer = DataTransformer(self.server_name, table_config)
        transformed_data = transformer.transform(data)

        # Track parent IDs for this table and write output
        self._write_output_stream(table_name, transformed_data, table_config)

        elapsed = time.time() - start_time
        logging.info(f"Table {table_name}: finished. Time elapsed: {elapsed:.2f} seconds.")

    async def _extract_child_table(self, client: DaktelaClient, table_name: str) -> None:
        """Extract a child table that depends on parent table."""
        table_config = get_table_config(table_name)
        if not table_config or not table_config.has_requirements():
            return

        start_time = time.time()

        parent_table = table_config.get_requirement_table()
        parent_column = table_config.get_requirement_column()
        child_table = table_config.get_child_table_name()

        parent_ids = self._get_parent_ids(parent_table, parent_column)

        valid_parent_ids = [pid for pid in parent_ids if pid not in self.invalid_activities]

        if not valid_parent_ids:
            logging.warning(f"No valid parent IDs for table {table_name}. Skipping.")
            return

        data = await client.extract_child_table(
            table_config.get_api_table_name(),
            valid_parent_ids,
            child_table
        )

        transformer = DataTransformer(self.server_name, table_config)
        transformed_data = transformer.transform(data)

        self._write_output_stream(table_name, transformed_data, table_config)

        elapsed = time.time() - start_time
        logging.info(f"Table {table_name}: finished. Time elapsed: {elapsed:.2f} seconds.")

    def _prepare_filters(self, table_config: TableConfig) -> list[dict[str, Any]]:
        """Prepare API filters with date range."""
        filters = []
        date_from = self.config.get_date_from()
        date_to = self.config.get_date_to()

        for f in table_config.filters:
            filter_copy = f.copy()
            if filter_copy.get("value") is None:
                if filter_copy.get("operator") == "gte":
                    filter_copy["value"] = date_from.strftime("%Y-%m-%d %H:%M:%S")
                elif filter_copy.get("operator") == "lte":
                    filter_copy["value"] = date_to.strftime("%Y-%m-%d %H:%M:%S")
            filters.append(filter_copy)

        return filters

    def _get_parent_ids(self, parent_table: str, parent_column: str) -> list[str]:
        """Get parent IDs from extracted data or CSV file."""
        # First check if we have parent IDs in memory
        normalized_col = parent_column.replace(".", "_")
        if parent_table in self.extracted_parent_ids:
            if normalized_col in self.extracted_parent_ids[parent_table]:
                return list(self.extracted_parent_ids[parent_table][normalized_col])

        # Otherwise, read from CSV file
        output_name = f"{self.server_name}_{parent_table}.csv"
        csv_path = Path(self.tables_out_path) / output_name
        if not csv_path.exists():
            return []

        try:
            # Read manifest to get column names
            manifest_path = Path(f"{csv_path}.manifest")
            if not manifest_path.exists():
                return []

            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = self._read_json_with_null_handling(f)
                columns = manifest.get("columns", [])
                if not columns or normalized_col not in columns:
                    return []

                col_index = columns.index(normalized_col)

            # Read CSV with null character handling
            parent_ids = set()
            with open(csv_path, 'r', encoding='utf-8') as f:
                lazy_lines = (line.replace('\0', '') for line in f)
                reader = csv.reader(lazy_lines)
                for row in reader:
                    if len(row) > col_index and row[col_index]:
                        parent_ids.add(row[col_index])

            return list(parent_ids)

        except Exception as e:
            logging.warning(f"Error reading parent table {parent_table}: {type(e).__name__}")
            return []

    @staticmethod
    def _read_json_with_null_handling(file_handle) -> dict:
        """Read JSON file with null character handling."""
        import json
        content = file_handle.read().replace('\0', '')
        return json.loads(content)

    def _write_output_stream(
        self,
        table_name: str,
        data_generator,
        table_config: TableConfig
    ) -> None:
        """Write transformed data to CSV using ElasticDictWriter."""
        output_name = f"{self.server_name}_{table_name}.csv"
        output_path = Path(self.tables_out_path) / output_name

        # Always use "id" as primary key since we generate it
        primary_key_list = ["id"]

        row_count = 0
        parent_ids_tracking = {}
        fieldnames = []

        try:
            # Collect rows and determine fieldnames dynamically
            rows = []
            for row in data_generator:
                rows.append(row)

                # Track new fieldnames
                for key in row.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

                # Track parent IDs for later use
                for key, value in row.items():
                    if value and value != "":
                        if key not in parent_ids_tracking:
                            parent_ids_tracking[key] = set()
                        parent_ids_tracking[key].add(str(value))

                # Track invalid activities
                if table_name == "activities":
                    self._track_invalid_activity_from_row(row, table_config)

            if not rows:
                logging.warning(f"No data extracted for table {table_name}")
                return

            # Write using ElasticDictWriter
            writer = ElasticDictWriter(str(output_path), fieldnames=fieldnames)
            for row in rows:
                writer.writerow(row)
                row_count += 1

            # Store parent IDs for this table
            self.extracted_parent_ids[table_name] = parent_ids_tracking

            # Create table definition with proper manifest
            table_def = self.create_out_table_definition(
                output_name,
                incremental=self.config.incremental,
                primary_key=primary_key_list
            )

            # Write manifest using Keboola component method
            self.write_manifest(table_def)

            logging.info(f"Written {row_count} rows to {output_name}")

        except Exception as e:
            logging.error(f"Error writing output for table {table_name}: {type(e).__name__}")
            raise

    def _track_invalid_activity_from_row(self, row: dict[str, Any], table_config: TableConfig) -> None:
        """Track invalid activities from a single row."""
        primary_key_col = table_config.primary_keys[0] if table_config.primary_keys else "name"
        normalized_col = primary_key_col.replace(".", "_")

        if normalized_col in row:
            value = row[normalized_col]
            if value is None or value == "":
                # Track the ID if it has one
                if "id" in row and row["id"]:
                    self.invalid_activities.append(str(row["id"]))


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.error(exc)
        sys.exit(1)
    except Exception as exc:
        logging.exception(exc)
        sys.exit(2)
