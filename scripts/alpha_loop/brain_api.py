#!/usr/bin/env python3
"""
WorldQuant BRAIN API 客户端 - 完整封装。
支持：认证、仿真、轮询、查询 Alpha 列表、获取详情、提交审核。
仅使用 Python 标准库。
"""
import json
import base64
import time
import os
import urllib.request
import urllib.error
import http.cookiejar
from pathlib import Path
from datetime import datetime, timezone

BASE = "https://api.worldquantbrain.com"
DEFAULT_SETTINGS = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 6,
    "neutralization": "SUBINDUSTRY",
    "truncation": 0.08,
    "maxTrade": "ON",
    "pasteurization": "ON",
    "testPeriod": "P1Y6M",
    "unitHandling": "VERIFY",
    "nanHandling": "OFF",
    "language": "FASTEXPR",
    "visualization": False,
}


class BrainAPI:
    def __init__(self, email: str = None, password: str = None, creds_file: str = None):
        self.opener = None
        if email and password:
            self._email = email
            self._password = password
        elif creds_file:
            with open(creds_file, 'r', encoding='utf-8') as f:
                creds = json.load(f)
            self._email, self._password = creds[0], creds[1]
        else:
            for path in ['~/.brain_credentials', '~/.openclaw/.brain_credentials']:
                p = os.path.expanduser(path)
                if os.path.exists(p):
                    with open(p, 'r', encoding='utf-8') as f:
                        creds = json.load(f)
                    self._email, self._password = creds[0], creds[1]
                    break
            else:
                raise FileNotFoundError("未找到 ~/.brain_credentials 或 ~/.openclaw/.brain_credentials")

    def authenticate(self):
        jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        cred = base64.b64encode(f"{self._email}:{self._password}".encode()).decode()
        req = urllib.request.Request(
            f"{BASE}/authentication",
            method="POST",
            headers={"Authorization": f"Basic {cred}", "Content-Type": "application/json"},
            data=b"{}",
        )
        self.opener.open(req)
        return self

    def _ensure_auth(self):
        if not self.opener:
            self.authenticate()

    def _request(self, url: str, method: str = "GET", data: dict = None, retry: int = 3) -> dict:
        self._ensure_auth()
        headers = {"Content-Type": "application/json"}
        body = json.dumps(data).encode() if data else None
        full_url = url if url.startswith("http") else f"{BASE}{url}"

        for attempt in range(retry):
            try:
                req = urllib.request.Request(full_url, method=method, headers=headers, data=body)
                with self.opener.open(req) as r:
                    raw = r.read().decode()
                    return json.loads(raw) if raw.strip() else {}
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt < retry - 1:
                    self.authenticate()
                    continue
                if e.code == 429:
                    wait = int(e.headers.get("Retry-After", 10))
                    time.sleep(wait)
                    continue
                raise
            except Exception:
                if attempt < retry - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise

    def _request_raw(self, url: str, method: str = "GET", data: dict = None):
        """返回 response 对象，用于需要读取 headers 的场景"""
        self._ensure_auth()
        headers = {"Content-Type": "application/json"}
        body = json.dumps(data).encode() if data else None
        full_url = url if url.startswith("http") else f"{BASE}{url}"
        req = urllib.request.Request(full_url, method=method, headers=headers, data=body)
        return self.opener.open(req)

    # ── 仿真 ──────────────────────────────────────────────

    def simulate(self, expression: str, settings: dict = None) -> str:
        """提交仿真，返回 progress_url"""
        self._ensure_auth()
        merged = dict(DEFAULT_SETTINGS)
        if settings:
            for k, v in settings.items():
                if k in ("region", "universe"):
                    merged[k] = v
                else:
                    merged[k] = v

        payload = {
            "type": "REGULAR",
            "regular": expression,
            "settings": merged,
        }
        req = urllib.request.Request(
            f"{BASE}/simulations",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode(),
        )
        try:
            with self.opener.open(req) as r:
                loc = r.headers.get("Location", "")
        except urllib.error.HTTPError as e:
            if e.code == 401:
                self.authenticate()
                return self.simulate(expression, settings)
            raise

        if not loc:
            raise RuntimeError("API 未返回 Location")
        return loc if loc.startswith("http") else f"{BASE}{loc}"

    def poll(self, progress_url: str, timeout: int = 300) -> tuple:
        """轮询仿真结果，返回 (alpha_id, data)"""
        start = time.time()
        while time.time() - start < timeout:
            try:
                data = self._request(progress_url)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(10)
                    continue
                raise

            status = data.get("status", "")
            if status == "DONE" or status == "COMPLETED" or data.get("retry-after") == 0:
                alpha_id = data.get("alpha") or data.get("alphaId") or data.get("alpha_id") or ""
                return alpha_id, data
            if status == "ERROR" or status == "FAILED":
                raise RuntimeError(f"仿真失败: {data.get('message', data)}")

            wait = min(int(data.get("retry-after", 5)), 15)
            time.sleep(max(1, wait))

        raise TimeoutError(f"仿真超时 ({timeout}s)")

    def simulate_and_wait(self, expression: str, settings: dict = None, timeout: int = 300) -> dict:
        """一步完成：提交仿真 → 轮询 → 返回 Alpha 详情"""
        url = self.simulate(expression, settings)
        alpha_id, poll_data = self.poll(url, timeout=timeout)
        if alpha_id:
            try:
                return self.get_alpha(alpha_id)
            except Exception:
                return poll_data
        return poll_data

    # ── Alpha 查询 ────────────────────────────────────────

    def get_alpha(self, alpha_id: str) -> dict:
        return self._request(f"/alphas/{alpha_id}")

    def list_alphas(self, limit: int = 100, offset: int = 0, order: str = "-dateCreated") -> list:
        """查询自己的 Alpha 列表"""
        result = self._request(f"/users/self/alphas?limit={limit}&offset={offset}&order={order}")
        if isinstance(result, list):
            return result
        return result.get("results", result.get("alphas", []))

    def get_alpha_correlations(self, alpha_id: str) -> list:
        """获取 Alpha 的自相关检查结果"""
        try:
            alpha = self.get_alpha(alpha_id)
            checks = alpha.get("is", {}).get("checks", [])
            return [c for c in checks if c.get("name") == "SELF_CORRELATION"]
        except Exception:
            return []

    # ── 提交 ──────────────────────────────────────────────

    def submit_alpha(self, alpha_id: str) -> dict:
        """提交 Alpha 到平台审核"""
        try:
            return self._request(f"/alphas/{alpha_id}/submit", method="POST")
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            raise RuntimeError(f"提交失败 HTTP {e.code}: {body}")

    # ── 工具 ──────────────────────────────────────────────

    def get_data_fields(self) -> list:
        """获取可用的数据字段列表"""
        try:
            return self._request("/data-fields?instrumentType=EQUITY&region=USA&delay=1&universe=TOP3000")
        except Exception:
            return []

    @staticmethod
    def extract_metrics(alpha: dict) -> dict:
        """从 Alpha 详情中提取关键指标"""
        is_data = alpha.get("is", {})
        if not isinstance(is_data, dict):
            is_data = {}
        checks = is_data.get("checks", [])
        self_corr = None
        for c in checks if isinstance(checks, list) else []:
            if c.get("name") == "SELF_CORRELATION":
                self_corr = c.get("value")
                break

        return {
            "alpha_id": alpha.get("id", ""),
            "sharpe": _safe_float(is_data.get("sharpe")),
            "fitness": _safe_float(is_data.get("fitness")),
            "turnover": _safe_float(is_data.get("turnover")),
            "returns": _safe_float(is_data.get("returns")),
            "drawdown": _safe_float(is_data.get("drawdown")),
            "margin": _safe_float(is_data.get("margin")),
            "self_correlation": _safe_float(self_corr),
            "status": alpha.get("status", ""),
            "date_created": alpha.get("dateCreated", ""),
            "date_submitted": alpha.get("dateSubmitted"),
        }


def _safe_float(val, default=0.0):
    if val is None or val == "N/A":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()
