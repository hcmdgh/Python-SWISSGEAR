from bson.json_util import loads as _loads
from bson.json_util import dumps as _dumps
from typing import Any 


def json_load(json_str: str) -> Any:
    return _loads(json_str)


def json_dump(obj: Any) -> str:
    return _dumps(obj, ensure_ascii=False)
