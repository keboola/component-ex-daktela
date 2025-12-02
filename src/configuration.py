import logging
import re
from datetime import datetime, timedelta
from typing import Optional

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class Configuration(BaseModel):
    username: str
    password: str = Field(alias="#password")
    url: Optional[str] = None
    server: Optional[str] = None
    date_from: str = Field(alias="from")
    date_to: str = Field(alias="to")
    tables: list[str]
    incremental: bool = False
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = []
            for err in e.errors():
                loc = err.get("loc", ())
                msg = err.get("msg", "Unknown error")
                if loc:
                    error_messages.append(f"{loc[0]}: {msg}")
                else:
                    error_messages.append(msg)
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

    @model_validator(mode="after")
    def validate_url_or_server(self):
        if not self.url and not self.server:
            raise ValueError("Either 'url' or 'server' parameter must be provided")
        return self

    @field_validator("url")
    @classmethod
    def validate_url_format(cls, v):
        if v is not None and v.strip():
            pattern = r"^https?://.+\.daktela\.com/?$"
            if not re.match(pattern, v):
                raise ValueError(
                    f"Invalid URL format: {v}. Expected format: https://{{server}}.daktela.com"
                )
        return v

    def get_base_url(self) -> str:
        if self.url:
            url = self.url.rstrip("/")
            return url
        return f"https://{self.server}.daktela.com"

    def get_server_name(self) -> str:
        if self.server:
            return self.server
        if self.url:
            match = re.search(r"https?://([^.]+)\.daktela\.com", self.url)
            if match:
                return match.group(1)
        raise UserException("Could not extract server name from URL")

    def get_table_list(self) -> list[str]:
        return [t.strip() for t in self.tables if t.strip()]

    def parse_date(self, date_str: str) -> datetime:
        date_str = date_str.strip()
        if date_str.lower() == "today" or date_str == "0":
            return datetime.now() - timedelta(minutes=30)
        if date_str.startswith("-") and date_str[1:].isdigit():
            days = int(date_str)
            return datetime.now() + timedelta(days=days)
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise UserException(
                f"Invalid date format: {date_str}. "
                f"Expected 'today', '0', negative integer (e.g., '-7'), or 'YYYY-MM-DD'"
            )

    def get_date_from(self) -> datetime:
        return self.parse_date(self.date_from)

    def get_date_to(self) -> datetime:
        return self.parse_date(self.date_to)

    def validate_date_range(self) -> None:
        date_from = self.get_date_from()
        date_to = self.get_date_to()
        if date_from >= date_to:
            raise UserException(
                f"Start date ({date_from.strftime('%Y-%m-%d')}) must be before "
                f"end date ({date_to.strftime('%Y-%m-%d')})"
            )
