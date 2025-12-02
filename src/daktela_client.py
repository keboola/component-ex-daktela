import asyncio
import logging
import ssl
from typing import Optional

import aiohttp
from keboola.component.exceptions import UserException


class DaktelaClient:
    DEFAULT_LIMIT = 1000
    MAX_RETRIES = 8

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self._session = aiohttp.ClientSession(connector=connector)
        await self.authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def authenticate(self) -> None:
        login_url = f"{self.base_url}/api/v6/login.json"
        params = {
            "username": self.username,
            "password": self.password,
            "only_token": "1"
        }

        try:
            async with self._session.post(login_url, params=params) as response:
                if response.status != 200:
                    raise UserException(
                        f"Invalid response from Daktela server. Status code: {response.status}. "
                        f"Reason: {response.reason}. Make sure your credentials are correct."
                    )
                data = await response.json()
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
                logging.info("Successfully authenticated with Daktela API")
        except aiohttp.ClientConnectorError:
            raise UserException("Server not responding, check your url.")
        except aiohttp.ClientError as e:
            raise UserException(f"Connection error: {str(e)}")

    async def _request_with_retry(
        self,
        url: str,
        params: Optional[dict] = None
    ) -> dict:
        if params is None:
            params = {}
        params["accessToken"] = self.access_token

        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                async with self._session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    last_error = f"Status {response.status}: {response.reason}"
            except (aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
                last_error = str(e)

            wait_time = attempt + 1
            logging.warning(
                f"Request failed (attempt {attempt + 1}/{self.MAX_RETRIES}): {last_error}. "
                f"Retrying in {wait_time}s..."
            )
            await asyncio.sleep(wait_time)

        raise UserException(f"Request failed after {self.MAX_RETRIES} retries: {last_error}")

    async def get_table_count(self, table_name: str, filters: Optional[list] = None) -> int:
        url = f"{self.base_url}/api/v6/{table_name}.json"
        params = {"skip": 0, "take": 1}
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
        url = f"{self.base_url}/api/v6/{table_name}.json"
        params = {"skip": skip, "take": limit}

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
        url = f"{self.base_url}/api/v6/{parent_table}/{parent_id}/{child_table}.json"
        params = {"skip": skip, "take": limit}

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("data", [])

    async def get_child_table_count(
        self,
        parent_table: str,
        parent_id: str,
        child_table: str
    ) -> int:
        url = f"{self.base_url}/api/v6/{parent_table}/{parent_id}/{child_table}.json"
        params = {"skip": 0, "take": 1}

        data = await self._request_with_retry(url, params)
        return data.get("result", {}).get("total", 0)

    def _build_filter_params(self, filters: list[dict]) -> str:
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
