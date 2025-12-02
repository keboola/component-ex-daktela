import hashlib
import re
from typing import Any, Generator

from table_config import TableConfig


class DataTransformer:
    """Transform raw API data into normalized format suitable for CSV output."""

    def __init__(self, server_name: str, table_config: TableConfig):
        self.server_name = server_name
        self.table_config = table_config
        self.html_pattern = re.compile(r"<.*?>")
        self.invalid_activities: list[str] = []

    def transform(self, data: list[dict]) -> Generator[dict[str, Any], None, None]:
        """
        Transform raw API data into normalized rows.

        Yields transformed rows one at a time for memory efficiency.
        """
        if not data:
            return

        for row in data:
            # Process the row through all transformation steps
            normalized_row = self._normalize_nested_json(row)
            filtered_row = self._filter_columns(normalized_row)

            # Handle list columns - may yield multiple rows per input row
            expanded_rows = self._handle_list_columns(filtered_row)
            for expanded_row in expanded_rows:
                # Handle list of dicts - may yield multiple rows
                further_expanded = self._handle_list_of_dicts_columns(expanded_row)
                for final_row in further_expanded:
                    cleaned_row = self._clean_html(final_row)
                    cleaned_row = self._add_server_column(cleaned_row)
                    cleaned_row = self._prefix_key_columns(cleaned_row)
                    cleaned_row = self._generate_compound_id(cleaned_row)
                    yield cleaned_row

    def _normalize_nested_json(self, row: dict) -> dict:
        """Flatten nested dictionaries with dot notation into underscore notation."""
        result = {}

        for key, value in row.items():
            normalized_key = key.replace(".", "_")

            if isinstance(value, dict):
                # Flatten nested dict
                for nested_key, nested_value in value.items():
                    nested_normalized = f"{normalized_key}_{nested_key}".replace(".", "_")
                    result[nested_normalized] = nested_value
            else:
                result[normalized_key] = value

        return result

    def _filter_columns(self, row: dict) -> dict:
        """Keep only configured columns if specified."""
        if not self.table_config.columns:
            return row

        normalized_columns = {col.replace(".", "_") for col in self.table_config.columns}
        return {k: v for k, v in row.items() if k in normalized_columns}

    def _handle_list_columns(self, row: dict) -> Generator[dict, None, None]:
        """Explode list columns - one row becomes multiple rows."""
        list_cols_to_explode = [
            col.replace(".", "_") for col in self.table_config.list_columns
        ]

        # Find which columns in this row need exploding
        cols_to_explode = [col for col in list_cols_to_explode if col in row]

        if not cols_to_explode:
            yield row
            return

        # Get the first list column to explode
        explode_col = cols_to_explode[0]
        list_values = row.get(explode_col, [])

        if not isinstance(list_values, list) or not list_values:
            yield row
            return

        # Create a row for each value in the list
        for value in list_values:
            new_row = row.copy()
            new_row[explode_col] = value

            # Recursively handle remaining list columns
            if len(cols_to_explode) > 1:
                # Temporarily modify config to handle remaining columns
                remaining = [c for c in cols_to_explode if c != explode_col]
                for sub_row in self._handle_list_columns_recursive(new_row, remaining):
                    yield sub_row
            else:
                yield new_row

    def _handle_list_columns_recursive(self, row: dict, remaining_cols: list[str]) -> Generator[dict, None, None]:
        """Helper to recursively explode multiple list columns."""
        if not remaining_cols:
            yield row
            return

        explode_col = remaining_cols[0]
        list_values = row.get(explode_col, [])

        if not isinstance(list_values, list) or not list_values:
            yield row
            return

        for value in list_values:
            new_row = row.copy()
            new_row[explode_col] = value

            if len(remaining_cols) > 1:
                yield from self._handle_list_columns_recursive(new_row, remaining_cols[1:])
            else:
                yield new_row

    def _handle_list_of_dicts_columns(self, row: dict) -> Generator[dict, None, None]:
        """Expand list-of-dicts columns into multiple rows with flattened keys."""
        list_of_dicts_cols = [
            col.replace(".", "_") for col in self.table_config.list_of_dicts_columns
        ]

        cols_to_expand = [col for col in list_of_dicts_cols if col in row]

        if not cols_to_expand:
            yield row
            return

        # Get first column to expand
        expand_col = cols_to_expand[0]
        list_values = row.get(expand_col, [])

        if not isinstance(list_values, list) or not list_values:
            # Remove the column and yield the row
            new_row = {k: v for k, v in row.items() if k != expand_col}
            yield new_row
            return

        # Create a row for each dict in the list
        for item in list_values:
            new_row = {k: v for k, v in row.items() if k != expand_col}

            if isinstance(item, dict):
                # Flatten the dict into the row with prefixed keys
                for k, v in item.items():
                    new_row[f"{expand_col}_{k}"] = v

            # Recursively handle remaining columns
            if len(cols_to_expand) > 1:
                remaining = [c for c in cols_to_expand if c != expand_col]
                yield from self._handle_list_of_dicts_columns_recursive(new_row, remaining)
            else:
                yield new_row

    def _handle_list_of_dicts_columns_recursive(self, row: dict, remaining_cols: list[str]) -> Generator[dict, None, None]:
        """Helper to recursively expand multiple list-of-dicts columns."""
        if not remaining_cols:
            yield row
            return

        expand_col = remaining_cols[0]
        list_values = row.get(expand_col, [])

        if not isinstance(list_values, list) or not list_values:
            new_row = {k: v for k, v in row.items() if k != expand_col}
            yield new_row
            return

        for item in list_values:
            new_row = {k: v for k, v in row.items() if k != expand_col}

            if isinstance(item, dict):
                for k, v in item.items():
                    new_row[f"{expand_col}_{k}"] = v

            if len(remaining_cols) > 1:
                yield from self._handle_list_of_dicts_columns_recursive(new_row, remaining_cols[1:])
            else:
                yield new_row

    def _clean_html(self, row: dict) -> dict:
        """Remove HTML tags from string values."""
        cleaned = {}
        for key, value in row.items():
            cleaned[key] = self._clean_html_value(value)
        return cleaned

    def _clean_html_value(self, value: Any) -> Any:
        """Remove HTML tags from a single value."""
        if not isinstance(value, str):
            return value

        cleaned = self.html_pattern.sub("", value)
        cleaned = cleaned.strip()

        if not cleaned:
            return None

        return cleaned

    def _add_server_column(self, row: dict) -> dict:
        """Add server name as first column."""
        return {"server": self.server_name, **row}

    def _prefix_key_columns(self, row: dict) -> dict:
        """Prefix key columns with server name."""
        all_key_columns = (
            self.table_config.primary_keys
            + self.table_config.secondary_keys
            + self.table_config.keys
        )

        result = {}
        for key, value in row.items():
            # Check if this column should be prefixed
            should_prefix = False
            for key_col in all_key_columns:
                if key_col.replace(".", "_") == key:
                    if key not in self.table_config.no_prefix_columns:
                        should_prefix = True
                    break

            if should_prefix and value is not None and value != "":
                result[key] = f"{self.server_name}_{value}"
            else:
                result[key] = value

        return result

    def _generate_compound_id(self, row: dict) -> dict:
        """Generate compound MD5 ID from primary and secondary keys."""
        primary_cols = [col.replace(".", "_") for col in self.table_config.primary_keys]
        secondary_cols = [col.replace(".", "_") for col in self.table_config.secondary_keys]

        all_key_cols = primary_cols + secondary_cols
        existing_key_cols = [col for col in all_key_cols if col in row]

        if not existing_key_cols:
            return {"id": "", **row}

        # Generate MD5 hash from key column values
        values = [str(row[col]) if row[col] is not None else "" for col in existing_key_cols]
        combined = "".join(values)
        id_hash = hashlib.md5(combined.encode()).hexdigest()

        # Insert id after server column
        result = {}
        result["server"] = row.get("server", "")
        result["id"] = id_hash
        result.update({k: v for k, v in row.items() if k not in ["server", "id"]})

        return result

    def track_invalid_activity(self, activity_id: str) -> None:
        """Track invalid activity IDs to filter out later."""
        self.invalid_activities.append(activity_id)

    def filter_invalid_activities(self, parent_ids: list[str]) -> list[str]:
        """Filter out invalid activity IDs from parent ID list."""
        return [pid for pid in parent_ids if pid not in self.invalid_activities]
