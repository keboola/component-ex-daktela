import hashlib
import re
from typing import Any

import pandas as pd

from table_config import TableConfig


class DataTransformer:
    def __init__(self, server_name: str, table_config: TableConfig):
        self.server_name = server_name
        self.table_config = table_config
        self.invalid_activities: list[str] = []

    def transform(self, data: list[dict]) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = self._normalize_nested_json(df)
        df = self._filter_columns(df)
        df = self._handle_list_columns(df)
        df = self._handle_list_of_dicts_columns(df)
        df = self._clean_html(df)
        df = self._add_server_column(df)
        df = self._prefix_key_columns(df)
        df = self._generate_compound_id(df)
        df = self._reorder_columns(df)

        return df

    def _normalize_nested_json(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                nested_df = pd.json_normalize(df[col].apply(lambda x: x if isinstance(x, dict) else {}))
                nested_df.columns = [
                    f"{col}_{c}" if not c.startswith(col) else c.replace(".", "_")
                    for c in nested_df.columns
                ]
                df = pd.concat([df.drop(columns=[col]), nested_df], axis=1)

        df.columns = [col.replace(".", "_") for col in df.columns]
        return df

    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.table_config.columns:
            return df

        normalized_columns = [col.replace(".", "_") for col in self.table_config.columns]
        existing_columns = [col for col in normalized_columns if col in df.columns]

        if not existing_columns:
            return df

        return df[existing_columns]

    def _handle_list_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.table_config.list_columns:
            normalized_col = col.replace(".", "_")
            if normalized_col in df.columns:
                df = df.explode(normalized_col)

        return df

    def _handle_list_of_dicts_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.table_config.list_of_dicts_columns:
            normalized_col = col.replace(".", "_")
            if normalized_col in df.columns:
                expanded_rows = []
                for idx, row in df.iterrows():
                    value = row[normalized_col]
                    if isinstance(value, list) and value:
                        for item in value:
                            new_row = row.copy()
                            if isinstance(item, dict):
                                for k, v in item.items():
                                    new_row[f"{normalized_col}_{k}"] = v
                            expanded_rows.append(new_row)
                    else:
                        expanded_rows.append(row)

                if expanded_rows:
                    df = pd.DataFrame(expanded_rows)
                    if normalized_col in df.columns:
                        df = df.drop(columns=[normalized_col])

        return df

    def _clean_html(self, df: pd.DataFrame) -> pd.DataFrame:
        html_pattern = re.compile(r"<.*?>")

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: self._clean_html_value(x, html_pattern))

        return df

    def _clean_html_value(self, value: Any, pattern: re.Pattern) -> Any:
        if not isinstance(value, str):
            return value

        cleaned = pattern.sub("", value)
        cleaned = cleaned.strip()

        if not cleaned:
            return None

        return cleaned

    def _add_server_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df.insert(0, "server", self.server_name)
        return df

    def _prefix_key_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        all_key_columns = (
            self.table_config.primary_keys
            + self.table_config.secondary_keys
            + self.table_config.keys
        )

        for col in all_key_columns:
            normalized_col = col.replace(".", "_")
            if normalized_col in df.columns and normalized_col not in self.table_config.no_prefix_columns:
                df[normalized_col] = df[normalized_col].apply(
                    lambda x: f"{self.server_name}_{x}" if pd.notna(x) and x != "" else x
                )

        return df

    def _generate_compound_id(self, df: pd.DataFrame) -> pd.DataFrame:
        primary_cols = [col.replace(".", "_") for col in self.table_config.primary_keys]
        secondary_cols = [col.replace(".", "_") for col in self.table_config.secondary_keys]

        all_key_cols = primary_cols + secondary_cols
        existing_key_cols = [col for col in all_key_cols if col in df.columns]

        if not existing_key_cols:
            df.insert(1, "id", "")
            return df

        def generate_id(row):
            values = [str(row[col]) if pd.notna(row[col]) else "" for col in existing_key_cols]
            combined = "".join(values)
            return hashlib.md5(combined.encode()).hexdigest()

        df.insert(1, "id", df.apply(generate_id, axis=1))
        return df

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        priority_cols = ["server", "id"]
        other_cols = [col for col in df.columns if col not in priority_cols]
        return df[priority_cols + other_cols]

    def track_invalid_activity(self, activity_id: str) -> None:
        self.invalid_activities.append(activity_id)

    def filter_invalid_activities(self, parent_ids: list[str]) -> list[str]:
        return [pid for pid in parent_ids if pid not in self.invalid_activities]
