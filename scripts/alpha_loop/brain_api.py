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
            merged.update(settings)

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
        """提交 Alpha 到平台审核（competition=challenge）"""
        try:
            return self._request(f"/alphas/{alpha_id}/submit?competition=challenge", method="POST")
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            raise RuntimeError(f"提交失败 HTTP {e.code}: {body}")

    def check_submission_status(self, alpha_id: str) -> dict:
        """检查提交后的实际状态（区分 201 接受 vs 真正 ACTIVE）"""
        alpha = self.get_alpha(alpha_id)
        checks = alpha.get("is", {}).get("checks", []) or []
        self_corr_result = None
        self_corr_value = None
        for c in checks:
            if c.get("name") == "SELF_CORRELATION":
                self_corr_result = c.get("result")
                self_corr_value = c.get("value")
                break
        return {
            "alpha_id": alpha_id,
            "status": alpha.get("status", ""),
            "date_submitted": alpha.get("dateSubmitted"),
            "self_corr_result": self_corr_result,
            "self_corr_value": _safe_float(self_corr_value),
        }

    def poll_submission(self, alpha_id: str, timeout: int = 120, interval: int = 10) -> dict:
        """提交后持续轮询直到 SELF_CORRELATION 不再 PENDING"""
        start = time.time()
        while time.time() - start < timeout:
            status = self.check_submission_status(alpha_id)
            if status["self_corr_result"] and status["self_corr_result"] != "PENDING":
                return status
            if status["status"] == "ACTIVE":
                return status
            time.sleep(interval)
        return self.check_submission_status(alpha_id)

    def submit_and_track(self, alpha_id: str) -> dict:
        """提交 + 轮询完整生命周期"""
        submit_result = self.submit_alpha(alpha_id)
        time.sleep(5)
        return self.poll_submission(alpha_id, timeout=120)

    # ── 平台分析 ──────────────────────────────────────────

    def get_data_fields(self) -> list:
        """获取可用的数据字段列表"""
        try:
            result = self._request("/data-fields?instrumentType=EQUITY&region=USA&delay=1&universe=TOP3000")
            if isinstance(result, dict):
                return result.get("results", [])
            return result if isinstance(result, list) else []
        except Exception:
            return []

    def scan_alpha_pool(self, limit: int = 500) -> dict:
        """扫描账号 alpha 池，返回分析结果"""
        all_alphas = []
        offset = 0
        while offset < limit:
            batch_size = min(100, limit - offset)
            batch = self.list_alphas(limit=batch_size, offset=offset)
            if not batch:
                break
            all_alphas.extend(batch)
            offset += len(batch)
            if len(batch) < batch_size:
                break

        analysis = {
            "total": len(all_alphas),
            "by_status": {},
            "active_alphas": [],
            "near_pass": [],
            "high_sharpe_blocked": [],
            "expressions": {},
        }

        for a in all_alphas:
            status = a.get("status", "UNKNOWN")
            analysis["by_status"][status] = analysis["by_status"].get(status, 0) + 1

            metrics = self.extract_metrics(a)
            expr = a.get("regular", {})
            if isinstance(expr, dict):
                expr = expr.get("code", "")

            if status == "ACTIVE":
                analysis["active_alphas"].append({
                    **metrics, "expression": expr,
                })

            if (metrics["sharpe"] >= 1.10 and metrics["fitness"] >= 0.85
                    and metrics["self_correlation"] > 0.7):
                analysis["high_sharpe_blocked"].append({
                    **metrics, "expression": expr,
                })

            if (metrics["sharpe"] >= 1.0 and metrics["fitness"] >= 0.8
                    and metrics["self_correlation"] <= 0.7
                    and status != "ACTIVE"):
                analysis["near_pass"].append({
                    **metrics, "expression": expr,
                })

            if expr:
                analysis["expressions"][metrics.get("alpha_id", "")] = expr

        analysis["near_pass"].sort(key=lambda x: x["sharpe"], reverse=True)
        analysis["high_sharpe_blocked"].sort(key=lambda x: x["sharpe"], reverse=True)

        return analysis

    def find_mother_candidates(self, pool_analysis: dict = None, top_n: int = 5) -> list:
        """从 alpha 池中找出最佳母因子候选"""
        if pool_analysis is None:
            pool_analysis = self.scan_alpha_pool()

        candidates = []

        for a in pool_analysis.get("high_sharpe_blocked", []):
            if a["sharpe"] >= 1.25 and a["fitness"] >= 1.0:
                candidates.append({
                    **a,
                    "reason": "high_sharpe_blocked_by_correlation",
                    "priority": a["sharpe"] * 0.6 + a["fitness"] * 0.4,
                })

        for a in pool_analysis.get("near_pass", []):
            if a["sharpe"] >= 1.10:
                candidates.append({
                    **a,
                    "reason": "near_pass_low_correlation",
                    "priority": a["sharpe"] * 0.4 + a["fitness"] * 0.3 + (0.7 - a["self_correlation"]) * 0.3,
                })

        candidates.sort(key=lambda x: x["priority"], reverse=True)
        return candidates[:top_n]

    # ── 工具 ──────────────────────────────────────────────

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
