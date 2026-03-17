import json
from pathlib import Path
from typing import Dict, Any, List, Callable


class JsonlStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, row: Dict[str, Any]):
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def load_all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        rows = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def replace_all(self, rows: List[Dict[str, Any]]):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def update_rows(self, predicate: Callable[[Dict[str, Any]], bool], updater: Callable[[Dict[str, Any]], Dict[str, Any]]):
        rows = self.load_all()
        updated = []
        for row in rows:
            updated.append(updater(row) if predicate(row) else row)
        self.replace_all(updated)
        return updated


class JsonStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, obj: Dict[str, Any]):
        self.path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self, default=None):
        if not self.path.exists():
            return {} if default is None else default
        return json.loads(self.path.read_text(encoding="utf-8"))
