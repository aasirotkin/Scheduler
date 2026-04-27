#!/usr/bin/env python3

from typing import Any, Tuple

#чтобы id_sort_key не лежал в интерфейсном файле
def id_sort_key(x: str) -> Tuple[int, Any]:
    return (0, int(x)) if x.isdigit() else (1, x)
