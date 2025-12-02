import logging
from typing import Any, Optional

from keboola.component.exceptions import UserException
from keboola.http_client import AsyncHttpClient


class DaktelaClient:
    DEFAULT_LIMIT = 1000
    MAX_RETRIES = 8

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self._client: Optional[AsyncHttpClient] = None

    async def __aenter__(self):
        # Initialize AsyncHttpClient with retry configuration
        self._client = AsyncHttpClient(
            base_url=self.base_url,
            retries=self.MAX_RETRIES,
            backoff_factor=1,
            retry_status_codes=[429, 500, 502, 503, 504]
        )
        await self.authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # AsyncHttpClient cleanup
        if self._client:
            await self._client.close()
        return False

    async def authenticate(self) -> None:
        """Authenticate with Daktela API and configure client with access token."""
        login_url = "/api/v6/login.json"
        params: dict[str, str] = {
            "username": self.username,
            "password": self.password,
            "only_token": "1"
        }

        try:
            response = await self._client.post_raw(login_url, params=params)

            if response.status_code != 200:
                raise UserException(
                    f"Invalid response from Daktela server. Status code: {response.status_code}. "
                    f"Make sure your credentials are correct."
                )

            data = response.json()
            result = data.get("result")
            if not result:
                raise UserException("Token received was invalid or empty!")

            # Handle both dict response (v6 API) and string response (legacy)
            if isinstance(result, dict):
                token = result.get("accessToken")
            else:
                token = result

            if not token or not isinstance(token, str):
                raise UserException("Token received was invalid or empty!")

            self.access_token = token

            # Reinitialize client with access token in default params
            await self._client.close()
            self._client = AsyncHttpClient(
                base_url=self.base_url,
                retries=self.MAX_RETRIES,
                backoff_factor=1,
                retry_status_codes=[429, 500, 502, 503, 504],
                default_params={"accessToken": self.access_token}
            )

            logging.info("Successfully authenticated with Daktela API")
        except UserException:
            raise
        except Exception as e:
            # Don't include exception message as it might contain sensitive data
            error_type = type(e).__name__
            if "Connection" in error_type or "Timeout" in error_type:
                raise UserException("Server not responding, check your url.")
            raise UserException("Connection error while authenticating with Daktela API.")

    async def _request_with_retry(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None
    ) -> dict:
        """Make GET request with automatic retry logic.

        The accessToken is automatically added via default_params in AsyncHttpClient.
        """
        try:
            data = await self._client.get(url, params=params)
            return data
        except Exception as e:
            # AsyncHttpClient already handles retries and error handling
            raise UserException(f"Request to {url} failed: {type(e).__name__}")

    async def get_table_count(self, table_name: str, filters: Optional[list] = None) -> int:
        url = f"/api/v6/{table_name}.json"
        params: dict[str, Any] = {"skip": 0, "take": 1}
        if filters:
            params["filter"] = self._build_filter_params(filters)

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("total", 0)

    async def get_table_data(
        self,
        table_name: str,
        skip: int = 0,
        limit: int = DEFAULT_LIMIT,
        filters: Optional[list] = None,
        fields: Optional[list] = None
    ) -> list[dict]:
        url = f"/api/v6/{table_name}.json"
        params: dict[str, Any] = {"skip": skip, "take": limit}

        if filters:
            params["filter"] = self._build_filter_params(filters)
        if fields:
            params["fields"] = ",".join(fields)

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("data", [])

    async def get_child_table_data(
        self,
        parent_table: str,
        parent_id: str,
        child_table: str,
        skip: int = 0,
        limit: int = DEFAULT_LIMIT
    ) -> list[dict]:
        url = f"/api/v6/{parent_table}/{parent_id}/{child_table}.json"
        params: dict[str, Any] = {"skip": skip, "take": limit}

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("data", [])

    async def get_child_table_count(
        self,
        parent_table: str,
        parent_id: str,
        child_table: str
    ) -> int:
        url = f"/api/v6/{parent_table}/{parent_id}/{child_table}.json"
        params: dict[str, Any] = {"skip": 0, "take": 1}

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("total", 0)

    @staticmethod
    def _build_filter_params(filters: list[dict]) -> str:
        """Build API filter parameters string from filter list."""
        filter_parts = []
        for f in filters:
            field = f.get("field", "")
            operator = f.get("operator", "")
            value = f.get("value", "")
            if value is not None:
                filter_parts.append(f"{field}[{operator}]={value}")
        return "&".join(filter_parts)

    async def extract_table(
        self,
        table_name: str,
        filters: Optional[list] = None,
        fields: Optional[list] = None,
        limit: int = DEFAULT_LIMIT
    ) -> list[dict]:
        total = await self.get_table_count(table_name, filters)
        batches = (total + limit - 1) // limit if total > 0 else 1

        logging.info(f"Table {table_name}: started. A total of {total} entries ({batches} batches).")

        all_data = []
        for i in range(0, max(total, 1), limit):
            batch_data = await self.get_table_data(table_name, skip=i, limit=limit, filters=filters, fields=fields)
            all_data.extend(batch_data)

        return all_data

    async def extract_child_table(
        self,
        parent_table: str,
        parent_ids: list[str],
        child_table: str,
        limit: int = DEFAULT_LIMIT
    ) -> list[dict]:
        all_data = []
        total_entries = 0

        for parent_id in parent_ids:
            count = await self.get_child_table_count(parent_table, parent_id, child_table)
            total_entries += count

            for i in range(0, max(count, 1), limit):
                batch_data = await self.get_child_table_data(
                    parent_table, parent_id, child_table, skip=i, limit=limit
                )
                for item in batch_data:
                    item[f"{parent_table}_name"] = parent_id
                all_data.extend(batch_data)

        batches = (total_entries + limit - 1) // limit if total_entries > 0 else 1
        logging.info(
            f"Table {parent_table}_{child_table}: started. "
            f"A total of {total_entries} entries ({batches} batches)."
        )

        return all_data
