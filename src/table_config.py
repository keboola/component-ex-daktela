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
    "accounts": TableConfig(
        name="accounts",
        requested_json="accounts",
        columns=[
            "name", "title", "description", "type", "phone", "email", "website",
            "address", "city", "zip", "country", "user.name", "user.title",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["user.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "calls": TableConfig(
        name="calls",
        requested_json="calls",
        columns=[
            "name", "clid", "did", "direction", "disposition", "duration", "billsec",
            "recording", "queue.name", "queue.title", "user.name", "user.title",
            "contact.name", "contact.title", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["queue.name", "user.name", "contact.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "records": TableConfig(
        name="records",
        requested_json="records",
        columns=[
            "name", "title", "description", "status", "contact.name", "contact.title",
            "account.name", "account.title", "user.name", "user.title",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["contact.name", "account.name", "user.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "statuses": TableConfig(
        name="statuses",
        requested_json="statuses",
        columns=[
            "name", "title", "type", "color", "default", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "categories": TableConfig(
        name="categories",
        requested_json="categories",
        columns=[
            "name", "title", "description", "type", "parent.name", "parent.title",
            "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["parent.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "emails": TableConfig(
        name="emails",
        requested_json="emails",
        columns=[
            "name", "subject", "from", "to", "cc", "bcc", "body", "direction",
            "status", "queue.name", "queue.title", "user.name", "user.title",
            "contact.name", "contact.title", "ticket.name", "ticket.title",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["queue.name", "user.name", "contact.name", "ticket.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "chats": TableConfig(
        name="chats",
        requested_json="chats",
        columns=[
            "name", "message", "direction", "status", "queue.name", "queue.title",
            "user.name", "user.title", "contact.name", "contact.title",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["queue.name", "user.name", "contact.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "sms": TableConfig(
        name="sms",
        requested_json="sms",
        columns=[
            "name", "text", "from", "to", "direction", "status",
            "user.name", "user.title", "contact.name", "contact.title",
            "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["user.name", "contact.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "devices": TableConfig(
        name="devices",
        requested_json="devices",
        columns=[
            "name", "title", "type", "extension", "user.name", "user.title",
            "status", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["user.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "profiles": TableConfig(
        name="profiles",
        requested_json="profiles",
        columns=[
            "name", "title", "description", "permissions", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=["permissions"],
        no_prefix_columns=[]
    ),
    "pauses": TableConfig(
        name="pauses",
        requested_json="pauses",
        columns=[
            "name", "title", "description", "type", "productive", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "skills": TableConfig(
        name="skills",
        requested_json="skills",
        columns=[
            "name", "title", "description", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "groups": TableConfig(
        name="groups",
        requested_json="groups",
        columns=[
            "name", "title", "description", "members", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=["members"],
        no_prefix_columns=[]
    ),
    "fields": TableConfig(
        name="fields",
        requested_json="fields",
        columns=[
            "name", "title", "type", "entity", "required", "options", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=["options"],
        no_prefix_columns=[]
    ),
    "forms": TableConfig(
        name="forms",
        requested_json="forms",
        columns=[
            "name", "title", "description", "type", "fields", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=["fields"],
        no_prefix_columns=[]
    ),
    "templates": TableConfig(
        name="templates",
        requested_json="templates",
        columns=[
            "name", "title", "type", "subject", "body", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "schedules": TableConfig(
        name="schedules",
        requested_json="schedules",
        columns=[
            "name", "title", "description", "timezone", "rules", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=["rules"],
        no_prefix_columns=[]
    ),
    "holidays": TableConfig(
        name="holidays",
        requested_json="holidays",
        columns=[
            "name", "title", "date", "recurring", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "recordings": TableConfig(
        name="recordings",
        requested_json="recordings",
        columns=[
            "name", "duration", "call.name", "user.name", "user.title",
            "url", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["call.name", "user.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "files": TableConfig(
        name="files",
        requested_json="files",
        columns=[
            "name", "title", "filename", "size", "mime", "url",
            "ticket.name", "ticket.title", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["ticket.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "notes": TableConfig(
        name="notes",
        requested_json="notes",
        columns=[
            "name", "text", "user.name", "user.title", "ticket.name", "ticket.title",
            "contact.name", "contact.title", "edited", "created"
        ],
        filters=[{"field": "edited", "operator": "gte", "value": None}],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["user.name", "ticket.name", "contact.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "activities_statuses": TableConfig(
        name="activities_statuses",
        requested_json="activities_statuses",
        columns=[
            "name", "title", "type", "category", "color", "icon", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "activities_call": TableConfig(
        name="activities_call",
        requested_json=ChildTableConfig(
            parent="activities",
            child="call",
            requirements=TableRequirements(table="activities", column="name")
        ),
        columns=[
            "name", "clid", "did", "direction", "disposition", "duration", "billsec",
            "recording", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "activities_email": TableConfig(
        name="activities_email",
        requested_json=ChildTableConfig(
            parent="activities",
            child="email",
            requirements=TableRequirements(table="activities", column="name")
        ),
        columns=[
            "name", "subject", "from", "to", "cc", "bcc", "body", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "activities_chat": TableConfig(
        name="activities_chat",
        requested_json=ChildTableConfig(
            parent="activities",
            child="chat",
            requirements=TableRequirements(table="activities", column="name")
        ),
        columns=[
            "name", "message", "channel", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "activities_sms": TableConfig(
        name="activities_sms",
        requested_json=ChildTableConfig(
            parent="activities",
            child="sms",
            requirements=TableRequirements(table="activities", column="name")
        ),
        columns=[
            "name", "text", "from", "to", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "tickets_categories": TableConfig(
        name="tickets_categories",
        requested_json="tickets/categories",
        columns=[
            "name", "title", "description", "parent.name", "parent.title", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=["parent.name"],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "contacts_custom_fields": TableConfig(
        name="contacts_custom_fields",
        requested_json="contacts/custom_fields",
        columns=[
            "name", "title", "type", "value", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "accounts_custom_fields": TableConfig(
        name="accounts_custom_fields",
        requested_json="accounts/custom_fields",
        columns=[
            "name", "title", "type", "value", "edited", "created"
        ],
        filters=[],
        primary_keys=["name"],
        secondary_keys=[],
        keys=[],
        list_columns=[],
        list_of_dicts_columns=[],
        no_prefix_columns=[]
    ),
    "users_queues": TableConfig(
        name="users_queues",
        requested_json="users/queues",
        columns=[
            "user.name", "queue.name", "queue.title", "priority", "edited", "created"
        ],
        filters=[],
        primary_keys=["user.name", "queue.name"],
        secondary_keys=[],
        keys=["user.name", "queue.name"],
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
