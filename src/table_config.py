from dataclasses import dataclass, field
from typing import Optional, Union


@dataclass
class TableRequirements:
    table: str
    column: str


@dataclass
class ChildTableConfig:
    parent: str
    child: str
    requirements: TableRequirements


@dataclass
class TableConfig:
    name: str
    requested_json: Union[str, ChildTableConfig]
    columns: list[str] = field(default_factory=list)
    filters: list[dict] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    secondary_keys: list[str] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)
    list_columns: list[str] = field(default_factory=list)
    list_of_dicts_columns: list[str] = field(default_factory=list)
    no_prefix_columns: list[str] = field(default_factory=list)

    def has_requirements(self) -> bool:
        return isinstance(self.requested_json, ChildTableConfig)

    def get_api_table_name(self) -> str:
        if isinstance(self.requested_json, str):
            return self.requested_json
        return self.requested_json.parent

    def get_child_table_name(self) -> Optional[str]:
        if isinstance(self.requested_json, ChildTableConfig):
            return self.requested_json.child
        return None

    def get_requirement_table(self) -> Optional[str]:
        if isinstance(self.requested_json, ChildTableConfig):
            return self.requested_json.requirements.table
        return None

    def get_requirement_column(self) -> Optional[str]:
        if isinstance(self.requested_json, ChildTableConfig):
            return self.requested_json.requirements.column
        return None


DEFAULT_TABLE_CONFIGS: dict[str, TableConfig] = {
    "activities": TableConfig(
        name="activities",
        requested_json="activities",
        columns=[
            "name", "title", "description", "direction", "time", "time_open", "time_close",
            "stage", "action", "clid", "did", "queue.name", "queue.title", "user.name",
            "user.title", "contact.name", "contact.title", "account.name", "account.title",
            "ticket.name", "ticket.title", "campaign.name", "campaign.title", "call.name",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["queue.name", "user.name", "contact.name", "account.name", "ticket.name", "campaign.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "contacts": TableConfig(
        name="contacts",
        requested_json="contacts",
        columns=[
            "name", "title", "firstname", "lastname", "email", "phone", "mobile",
            "company", "position", "address", "city", "zip", "country", "description",
            "account.name", "account.title", "user.name", "user.title", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["account.name", "user.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "tickets": TableConfig(
        name="tickets",
        requested_json="tickets",
        columns=[
            "name", "title", "description", "stage", "priority", "sla_deadtime",
            "sla_change", "category.name", "category.title", "contact.name", "contact.title",
            "account.name", "account.title", "user.name", "user.title", "queue.name",
            "queue.title", "tags", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["category.name", "contact.name", "account.name", "user.name", "queue.name"],
        list_columns=["tags"],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "users": TableConfig(
        name="users",
        requested_json="users",
        columns=[
            "name", "title", "firstname", "lastname", "email", "phone", "mobile",
            "extension", "alias", "role.name", "role.title", "groups", "skills",
            "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["role.name"],
        list_columns=[],
        list_of_dicts_columns=["groups", "skills"],
        no_prefix_columns=[]
    ),
    "queues": TableConfig(
        name="queues",
        requested_json="queues",
        columns=[
            "name", "title", "description", "type", "strategy", "timeout", "wrapup_time",
            "max_waiting", "max_waiting_time", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "campaigns": TableConfig(
        name="campaigns",
        requested_json="campaigns",
        columns=[
            "name", "title", "description", "type", "status", "queue.name", "queue.title",
            "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["queue.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
}


def get_table_config(table_name: str, custom_configs: Optional[dict] = None) -> Optional[TableConfig]:
    if custom_configs and table_name in custom_configs:
        config_dict = custom_configs[table_name]
        return _dict_to_table_config(table_name, config_dict)
    return DEFAULT_TABLE_CONFIGS.get(table_name)


def _dict_to_table_config(name: str, config_dict: dict) -> TableConfig:
    requested_json = config_dict.get("requested_json", name)
    if isinstance(requested_json, dict):
        requested_json = ChildTableConfig(
            parent=requested_json.get("parent", ""),
            child=requested_json.get("child", ""),
            requirements=TableRequirements(
                table=requested_json.get("requirements", {}).get("table", ""),
                column=requested_json.get("requirements", {}).get("column", "")
            )
        )

    return TableConfig(
        name=name,
        requested_json=requested_json,
        columns=config_dict.get("columns", []),
        filters=config_dict.get("filters", []),
        primary_keys=config_dict.get("primary_keys", []),
        secondary_keys=config_dict.get("secondary_keys", []),
        keys=config_dict.get("keys", []),
        list_columns=config_dict.get("list_columns", []),
        list_of_dicts_columns=config_dict.get("list_of_dicts_columns", []),
        no_prefix_columns=config_dict.get("no_prefix_columns", [])
    )
