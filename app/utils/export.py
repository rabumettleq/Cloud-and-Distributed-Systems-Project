import os
import json
from typing import Any, Optional


def ensureDir(dirPath: str) -> None:
    os.makedirs(dirPath, exist_ok=True)


def saveJson(filePath: str, data: Any) -> None:
    dirPath = os.path.dirname(filePath)
    if dirPath:
        ensureDir(dirPath)

    with open(filePath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def readJson(filePath: str) -> Optional[Any]:
    if not os.path.exists(filePath):
        return None

    with open(filePath, "r", encoding="utf-8") as f:
        return json.load(f)
