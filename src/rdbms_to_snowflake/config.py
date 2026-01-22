import yaml
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_sf_config(data: Dict[str, Any]) -> SnowflakeConfig:
    sf = data.get("snowflake", {})
    return SnowflakeConfig(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf.get("schema", "PUBLIC"),
        role=sf.get("role"),
    )
