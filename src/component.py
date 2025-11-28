"""
Daktela Extractor Component for Keboola.

Extracts data from Daktela CRM/Contact Center system via API v6.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from daktela_client import DaktelaClient
from data_transformer import DataTransformer
from table_config import TableConfig, get_table_config


class Component(ComponentBase):
    """
    Daktela Extractor Component.

    Extracts data from Daktela API v6 and produces CSV output compatible
    with the existing revolt-bi.app-daktela component.
    """

    SPECIAL_TABLES = {"activities", "activities_statuses"}
    TABLES_WITH_REQUIREMENTS = {
        "activities_call", "activities_email", "activities_chat"
    }

    def __init__(self):
        super().__init__()
        self.config: Optional[Configuration] = None
        self.server_name: str = ""
        self.extracted_data: dict[str, pd.DataFrame] = {}
        self.invalid_activities: list[str] = []

    def run(self):
        """Main execution code"""
        start_time = time.time()

        self._init_configuration()
        self._validate_configuration()

        asyncio.run(self._extract_all_tables())

        elapsed = time.time() - start_time
        logging.info(f"Elapsed time of extraction: {elapsed:.2f} seconds.")

    def _init_configuration(self) -> None:
        self.config = Configuration(**self.configuration.parameters)
        self.server_name = self.config.get_server_name()

        image_params = self.configuration.image_parameters or {}
        self.custom_table_configs = self._get_custom_table_configs(image_params)

        if self.config.debug:
            logging.getLogger().setLevel(logging.DEBUG)

    def _validate_configuration(self) -> None:
        self.config.validate_date_range()

        requested_tables = self.config.get_table_list()
        for table_name in requested_tables:
            table_config = get_table_config(table_name, self.custom_table_configs)
            if not table_config:
                logging.warning(f"Table '{table_name}' is not configured. Skipping.")

    def _get_custom_table_configs(self, image_params: dict) -> Optional[dict]:
        fields_list = image_params.get("fields", [])
        if not fields_list:
            return None

        project_id = os.environ.get("KBC_PROJECTID", "")
        stack_id = os.environ.get("KBC_STACKID", "")
        full_project_id = f"{stack_id}-{project_id}" if stack_id and project_id else ""

        default_config = None
        project_config = None

        for field_config in fields_list:
            name = field_config.get("name", "")
            project_ids = field_config.get("project_ids", [])

            if name == "default":
                default_config = field_config.get("columns", {})
            elif full_project_id and full_project_id in project_ids:
                project_config = field_config.get("columns", {})

        if project_config and default_config:
            merged = default_config.copy()
            merged.update(project_config)
            return merged
        elif project_config:
            return project_config
        elif default_config:
            return default_config

        return None

    async def _extract_all_tables(self) -> None:
        requested_tables = self.config.get_table_list()

        tables_without_requirements = []
        tables_with_requirements = []

        for table_name in requested_tables:
            table_config = get_table_config(table_name, self.custom_table_configs)
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
            for table_name in tables_without_requirements:
                await self._extract_table(client, table_name)

            special_tables_to_extract = [
                t for t in ["activities", "activities_statuses"]
                if t in tables_with_requirements
            ]
            for table_name in special_tables_to_extract:
                await self._extract_table(client, table_name)

            remaining_tables = [
                t for t in tables_with_requirements
                if t not in self.SPECIAL_TABLES
            ]
            for table_name in remaining_tables:
                await self._extract_child_table(client, table_name)

    async def _extract_table(self, client: DaktelaClient, table_name: str) -> None:
        table_config = get_table_config(table_name, self.custom_table_configs)
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
        df = transformer.transform(data)

        if table_name == "activities":
            self._track_invalid_activities(df, table_config)
            if "name" in df.columns:
                df = df.rename(columns={"name": "activities_name"})

        self._write_output(table_name, df)
        self.extracted_data[table_name] = df

        elapsed = time.time() - start_time
        logging.info(f"Table {table_name}: finished. Time elapsed: {elapsed:.2f} seconds.")

    async def _extract_child_table(self, client: DaktelaClient, table_name: str) -> None:
        table_config = get_table_config(table_name, self.custom_table_configs)
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
        df = transformer.transform(data)

        self._write_output(table_name, df)
        self.extracted_data[table_name] = df

        elapsed = time.time() - start_time
        logging.info(f"Table {table_name}: finished. Time elapsed: {elapsed:.2f} seconds.")

    def _prepare_filters(self, table_config: TableConfig) -> list[dict]:
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
        if parent_table in self.extracted_data:
            df = self.extracted_data[parent_table]
            normalized_col = parent_column.replace(".", "_")
            if normalized_col in df.columns:
                return df[normalized_col].dropna().unique().tolist()

        csv_path = self._get_output_path(parent_table)
        if csv_path.exists():
            try:
                parent_df = pd.read_csv(csv_path, header=None)
                manifest_path = Path(f"{csv_path}.manifest")
                if manifest_path.exists():
                    with open(manifest_path) as f:
                        manifest = json.load(f)
                        columns = manifest.get("columns", [])
                        if columns:
                            parent_df.columns = columns
                            normalized_col = parent_column.replace(".", "_")
                            if normalized_col in parent_df.columns:
                                return parent_df[normalized_col].dropna().unique().tolist()
            except Exception as e:
                logging.warning(f"Error reading parent table {parent_table}: {e}")

        return []

    def _track_invalid_activities(self, df: pd.DataFrame, table_config: TableConfig) -> None:
        primary_key_col = table_config.primary_keys[0] if table_config.primary_keys else "name"
        normalized_col = primary_key_col.replace(".", "_")

        if normalized_col in df.columns:
            invalid_mask = df[normalized_col].isna() | (df[normalized_col] == "")
            invalid_ids = df.loc[invalid_mask, normalized_col].tolist()
            self.invalid_activities.extend([str(i) for i in invalid_ids if pd.notna(i)])

    def _write_output(self, table_name: str, df: pd.DataFrame) -> None:
        if df.empty:
            logging.warning(f"No data extracted for table {table_name}")
            return

        output_name = f"{self.server_name}_{table_name}.csv"
        table_def = self.create_out_table_definition(
            output_name,
            incremental=self.config.incremental,
            primary_key=["id"]
        )

        df.to_csv(table_def.full_path, index=False, header=False)

        manifest = {
            "columns": df.columns.tolist(),
            "primary_key": ["id"],
            "incremental": self.config.incremental
        }

        manifest_path = f"{table_def.full_path}.manifest"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logging.info(f"Written {len(df)} rows to {output_name}")

    def _get_output_path(self, table_name: str) -> Path:
        output_name = f"{self.server_name}_{table_name}.csv"
        return Path(self.tables_out_path) / output_name


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
