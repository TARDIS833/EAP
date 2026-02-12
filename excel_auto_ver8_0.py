# -*- coding: utf-8 -*-
"""엑셀 자동 변환 (자사몰/스마트스토어) - excel_auto_ver8_0."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sys
import tempfile
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import hashlib

import pandas as pd

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except Exception:
    tk = None


# =========================
# 공통 유틸
# =========================
VERSION = "8_0"

def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def setup_logging() -> None:
    """앱 디렉토리에 누적식 로그 파일을 설정한다."""
    log_path = get_app_dir() / f"excel_auto_ver{VERSION}.log"
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                          datefmt="%H:%M:%S")
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    # 중복 핸들러 방지
    if not root_logger.handlers:
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)


# 앱 시작 시 로그 초기화
setup_logging()
logger = logging.getLogger("excel_auto")


def _resolve_config_path(relative: str) -> Optional[Path]:
    """개발 환경과 PyInstaller onedir 빌드(_internal) 모두에서 설정 파일을 찾는다."""
    candidates = [
        get_app_dir() / relative,
        get_app_dir() / "_internal" / relative,
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


PATCH_SOURCE_FILENAME = "patch_source.json"


def get_patch_source_path() -> Path:
    return get_app_dir() / PATCH_SOURCE_FILENAME


def load_patch_source() -> Dict[str, Any]:
    default = {
        "manifest_url": "",
        "timeout_sec": 15,
    }
    path = get_patch_source_path()
    if not path.exists():
        path.write_text(json.dumps(default, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(data, dict):
        return default
    merged = dict(default)
    merged.update(data)
    return merged


def download_bytes(url: str, timeout_sec: int = 15) -> bytes:
    req = Request(url, headers={"User-Agent": "excel-auto-updater/8.0"})
    with urlopen(req, timeout=timeout_sec) as resp:
        return resp.read()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def backup_configs_dir(config_dir: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_root = get_app_dir() / "backups" / ts
    backup_root.mkdir(parents=True, exist_ok=True)
    if config_dir.exists():
        shutil.copytree(config_dir, backup_root / "configs", dirs_exist_ok=True)
    return backup_root


def apply_online_patch_update() -> Tuple[bool, str]:
    """
    온라인 패치를 다운로드해 configs를 교체한다.
    성공 시 (True, 메시지), 실패 시 (False, 오류메시지)
    """
    source = load_patch_source()
    manifest_url = normalize_text(source.get("manifest_url"))
    timeout_sec = int(source.get("timeout_sec") or 15)

    if not manifest_url:
        return False, (
            f"{PATCH_SOURCE_FILENAME}의 manifest_url이 비어 있습니다.\n"
            "Public 리포의 raw manifest URL을 입력해주세요."
        )

    try:
        manifest_bytes = download_bytes(manifest_url, timeout_sec=timeout_sec)
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (HTTPError, URLError) as e:
        return False, f"패치 서버 연결 실패: {e}"
    except Exception as e:
        return False, f"manifest 읽기 실패: {e}"

    if not isinstance(manifest, dict):
        return False, "manifest 형식 오류: object(dict) 여야 합니다."
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        return False, "manifest 형식 오류: files 목록이 비어 있습니다."

    base_url = manifest_url.rsplit("/", 1)[0]
    tmp_dir = Path(tempfile.mkdtemp(prefix="excel_patch_"))
    downloaded: List[Tuple[str, Path]] = []

    try:
        for entry in files:
            if not isinstance(entry, dict):
                return False, "manifest files 항목 형식 오류"
            rel_path = normalize_text(entry.get("path"))
            expected_hash = normalize_text(entry.get("sha256")).lower()
            if not rel_path or not expected_hash:
                return False, f"manifest files 항목 누락: {entry}"

            file_url = f"{base_url}/{rel_path}"
            data = download_bytes(file_url, timeout_sec=timeout_sec)
            actual_hash = sha256_hex(data).lower()
            if actual_hash != expected_hash:
                return False, f"해시 검증 실패: {rel_path}"

            local_path = tmp_dir / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(data)
            downloaded.append((rel_path, local_path))

        config_dir = get_app_dir() / "configs"
        config_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_configs_dir(config_dir)

        for rel_path, local_path in downloaded:
            if not rel_path.startswith("configs/"):
                continue
            target = get_app_dir() / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, target)

        # 적용 후 매핑 재로딩
        load_mappings()
        return True, (
            f"온라인 패치 적용 완료\n"
            f"- 버전: {normalize_text(manifest.get('patch_version')) or 'unknown'}\n"
            f"- 백업: {backup_path}"
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def get_default_input_dir() -> Path:
    candidate = get_app_dir() / "260208 data_Edited" / "export_sample"
    if candidate.exists():
        return candidate
    return get_app_dir() / "input"


def get_default_output_dir() -> Path:
    # 시스템 기본 다운로드 폴더
    downloads = Path.home() / "Downloads"
    if downloads.exists():
        return downloads
    candidate = get_app_dir() / "260208 data_Edited" / "export_auto"
    if candidate.exists():
        return candidate
    return get_app_dir() / "output"


def normalize_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    # 숫자형 값의 과학적 표기(E+)를 방지한다.
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        s = format(value, "f").rstrip("0").rstrip(".")
        return s
    if isinstance(value, int):
        return str(value)
    return str(value).strip()


def compress_filename_for_status(path_or_name: str, head: int = 12, tail: int = 8) -> str:
    name = Path(path_or_name).name
    stem = Path(name).stem  # 확장자는 표시하지 않는다.
    if len(stem) <= head + tail + 3:
        return stem
    return f"{stem[:head]}...{stem[-tail:]}"


def is_gift_like_rule_item(item: Dict[str, str]) -> bool:
    p = normalize_text(item.get("product"))
    c = normalize_text(item.get("color"))
    s = normalize_text(item.get("size"))
    combined = f"{p} {c} {s}"
    return ("증정" in combined) or ("사은품" in combined)


ID_COLUMN_KEYWORDS = (
    "상품주문번호",
    "주문번호",
    "송장번호",
    "상품번호",
    "원주문번호",
    "배송비 묶음번호",
)


def is_id_like_column(col_name: str) -> bool:
    name = normalize_text(col_name)
    return any(k in name for k in ID_COLUMN_KEYWORDS)


def normalize_id_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, "f").rstrip("0").rstrip(".")

    s = str(value).strip()
    if not s:
        return ""
    # 문자열 과학적 표기 방지: 2.123141E+15 -> 2123141000000000
    if re.search(r"[eE][+-]?\d+$", s):
        try:
            d = Decimal(s)
            if d == d.to_integral():
                return format(d, "f").split(".")[0]
            return format(d, "f").rstrip("0").rstrip(".")
        except (InvalidOperation, ValueError):
            return s
    # 12345.0 형태 ID 정리
    if re.match(r"^-?\d+\.0+$", s):
        return s.split(".")[0]
    return s


def enforce_id_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if is_id_like_column(str(col)):
            out[col] = out[col].map(normalize_id_value)
    return out


DEFAULT_DELIVERY_MESSAGES = {
    "배송 전에 미리 연락 바랍니다.",
}


# EXE 배포 기본 동작은 GUI 기준이다.
MANUAL_PROMPT_ENABLED = True


def normalize_delivery_message(value) -> str:
    msg = normalize_text(value)
    if msg in DEFAULT_DELIVERY_MESSAGES:
        return ""
    return msg


def detect_header_row(path: Path, key: str) -> int:
    raw = pd.read_excel(path, sheet_name=0, header=None, nrows=8)
    for i in range(min(8, len(raw))):
        row_vals = [normalize_text(v) for v in raw.iloc[i].tolist()]
        if any(key == cell for cell in row_vals):
            return i
    return 0


def normalize_cafe24_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Cafe24 실무 양식의 컬럼을 내부 표준 컬럼으로 정규화한다."""
    out = df.copy()
    if "옵션" in out.columns and "주문상품명(옵션포함)" not in out.columns:
        out["주문상품명(옵션포함)"] = out["옵션"]

    # 전화 컬럼은 우선순위로 단일화해 중복 컬럼 생성 문제를 방지한다.
    if "수령인 휴대전화" not in out.columns:
        phone_cols = [c for c in ["핸드폰", "전화번호"] if c in out.columns]
        if phone_cols:
            phone = out[phone_cols[0]].copy()
            for c in phone_cols[1:]:
                phone = phone.where(phone.notna() & (phone.astype(str).str.strip() != ""), out[c])
            out["수령인 휴대전화"] = phone

    if "수령인 주소" not in out.columns and "주소" in out.columns:
        out["수령인 주소"] = out["주소"]
    if "수령인 상세 주소" not in out.columns:
        out["수령인 상세 주소"] = ""
    if "배송메시지" not in out.columns:
        if "비고" in out.columns:
            out["배송메시지"] = out["비고"]
        else:
            out["배송메시지"] = ""
    if "수량" not in out.columns:
        out["수량"] = "1"

    # 동일 컬럼명이 중복되면 첫 컬럼만 유지한다.
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()]

    return out


def split_color_size(value: str) -> Tuple[str, str]:
    value = normalize_text(value)
    if "사이즈=" in value:
        # 잘못 전달된 "색상=...; 사이즈=..." 문자열에서 색상만 추출
        m = re.search(r"색상\s*=\s*([^,;/\)]+)", value)
        n = re.search(r"사이즈\s*=\s*([^,;/\)]+)", value)
        if m:
            return normalize_text(m.group(1)), normalize_text(n.group(1)) if n else "FREE"
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return "", ""

    # 색상/사이즈 구분
    tokens = value.split(" ")
    if len(tokens) == 1:
        return tokens[0], "FREE"

    size = tokens[-1]
    color = " ".join(tokens[:-1]).strip()
    if not color:
        color = size
        size = "FREE"
    return color, size


def is_prefixed_sku(value: str) -> bool:
    value = normalize_text(value)
    if not value:
        return False
    if " " in value:
        return False
    return bool(re.match(r"^[^_]+_[^_]+_[^_]+$", value))


def split_prefixed_sku(value: str) -> Tuple[str, str, str]:
    parts = value.split("_")
    if len(parts) >= 3:
        return parts[0], parts[1], "_".join(parts[2:])
    return value, "", ""


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# =========================
# 커스텀 룰 관리
# =========================

class CustomRuleStore:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, List[Dict[str, str]]] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, key: str) -> Optional[List[Dict[str, str]]]:
        return self.data.get(key)

    def set(self, key: str, rows: List[Dict[str, str]]) -> None:
        self.data[key] = rows
        self.save()


def get_gift_rules_path() -> Path:
    cfg_dir = get_app_dir() / "configs"
    ensure_dir(cfg_dir)
    return cfg_dir / "gift_rules.json"


def get_custom_rules_path() -> Path:
    cfg_dir = get_app_dir() / "configs"
    ensure_dir(cfg_dir)
    return cfg_dir / "custom_rules.json"


def default_gift_rules() -> Dict[str, Any]:
    return {
        "rules": [
            {
                "name": "로크 젤리브라 증정",
                "store": "공통",
                "match_field": "product",
                "contains": "[로크 젤리브라 증정]",
                "output": {
                    "product": "로크젤리B",
                    "color": "랜덤",
                    "size_mode": "selected",
                    "size_value": "",
                    "size_suffix": "(증정)",
                    "qty": "1",
                },
            },
            {
                "name": "헬씨 브리프 증정",
                "store": "공통",
                "match_field": "product",
                "contains": "[헬씨 브리프 증정]",
                "output": {
                    "product": "헬씨P",
                    "color": "랜덤",
                    "size_mode": "selected",
                    "size_value": "",
                    "size_suffix": "(증정)",
                    "qty": "1",
                },
            },
            {
                "name": "기프트박스 중",
                "store": "공통",
                "match_field": "option",
                "contains": "기프트박스 (중)",
                "output": {
                    "product": "타밈",
                    "color": "기프트패키지",
                    "size_mode": "fixed",
                    "size_value": "중(증정)",
                    "size_suffix": "",
                    "qty": "1",
                },
            },
            {
                "name": "기프트박스 대",
                "store": "공통",
                "match_field": "option",
                "contains": "기프트박스 (대)",
                "output": {
                    "product": "타밈",
                    "color": "기프트패키지",
                    "size_mode": "fixed",
                    "size_value": "대(증정)",
                    "size_suffix": "",
                    "qty": "1",
                },
            },
        ]
    }


def validate_gift_rules_payload(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    rules = payload.get("rules")
    if not isinstance(rules, list):
        return ["rules는 list여야 합니다."]
    for idx, rule in enumerate(rules):
        p = f"rules[{idx}]"
        if not isinstance(rule, dict):
            errors.append(f"{p}는 dict여야 합니다.")
            continue
        for k in ("name", "store", "match_field", "contains", "output"):
            if k not in rule:
                errors.append(f"{p}.{k} 누락")
        if not isinstance(rule.get("output"), dict):
            errors.append(f"{p}.output은 dict여야 합니다.")
            continue
        out = rule["output"]
        for k in ("product", "color", "size_mode", "size_value", "size_suffix", "qty"):
            if k not in out:
                errors.append(f"{p}.output.{k} 누락")
        if out.get("size_mode") not in ("selected", "fixed"):
            errors.append(f"{p}.output.size_mode는 selected 또는 fixed여야 합니다.")
    return errors


class GiftRuleStore:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Any] = {"rules": []}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.data = default_gift_rules()
            self.save()
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            payload = default_gift_rules()
        if not isinstance(payload, dict):
            payload = default_gift_rules()
        errs = validate_gift_rules_payload(payload)
        if errs:
            logger.warning("[사은품 규칙] 형식 오류로 기본값 복구: %s", "; ".join(errs))
            payload = default_gift_rules()
        self.data = payload
        self.save()

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def set_payload(self, payload: Dict[str, Any]) -> None:
        errs = validate_gift_rules_payload(payload)
        if errs:
            raise ValueError("\n".join(errs))
        self.data = payload
        self.save()

    def get_rules(self) -> List[Dict[str, Any]]:
        rules = self.data.get("rules", [])
        return [r for r in rules if isinstance(r, dict)]

    def reset_defaults(self) -> None:
        self.data = default_gift_rules()
        self.save()


# =========================
# 수동 입력 다이얼로그
# =========================

@dataclass
class ManualRow:
    product: str
    color: str
    size: str
    qty: str


class ManualRuleDialog:
    def __init__(self, root, title: str, info_lines: List[str]):
        self.root = root
        self.title = title
        self.info_lines = info_lines
        self.result: Optional[List[ManualRow]] = None
        self.rows: List[Tuple[tk.Entry, tk.Entry, tk.Entry, tk.Entry]] = []

        self.win = tk.Toplevel(root)
        self.win.title(title)
        self.win.geometry("1200x420")
        self.win.grab_set()

        info_frame = tk.Frame(self.win)
        info_frame.pack(fill="x", padx=10, pady=8)
        for line in info_lines:
            lbl = tk.Label(info_frame, text=line, anchor="w", justify="left")
            lbl.pack(fill="x")

        header = tk.Frame(self.win)
        header.pack(fill="x", padx=10)
        for text, width in [("제품명", 18), ("옵션(색상)", 15), ("사이즈", 5), ("수량", 5)]:
            lbl = tk.Label(header, text=text, width=width, anchor="w")
            lbl.pack(side="left", padx=2)

        self.rows_frame = tk.Frame(self.win)
        self.rows_frame.pack(fill="both", expand=True, padx=10, pady=5)

        btn_frame = tk.Frame(self.win)
        btn_frame.pack(fill="x", padx=10, pady=8)
        tk.Button(btn_frame, text="+ 행 추가", command=self.add_row).pack(side="left")
        tk.Button(btn_frame, text="확인", command=self.on_submit).pack(side="right")
        tk.Button(btn_frame, text="취소", command=self.on_cancel).pack(side="right", padx=5)

        self.add_row()

    def add_row(self) -> None:
        row = tk.Frame(self.rows_frame)
        row.pack(fill="x", pady=2)
        e_product = tk.Entry(row, width=22)
        e_color = tk.Entry(row, width=16)
        e_size = tk.Entry(row, width=12)
        e_qty = tk.Entry(row, width=8)
        e_product.pack(side="left", padx=2)
        e_color.pack(side="left", padx=2)
        e_size.pack(side="left", padx=2)
        e_qty.pack(side="left", padx=2)
        tk.Button(row, text="-", command=lambda: self.remove_row(row)).pack(side="left", padx=4)
        self.rows.append((e_product, e_color, e_size, e_qty))

    def remove_row(self, row_frame) -> None:
        row_frame.destroy()
        self.rows = [r for r in self.rows if r[0].winfo_exists()]

    def on_submit(self) -> None:
        results: List[ManualRow] = []
        for e_product, e_color, e_size, e_qty in self.rows:
            product = normalize_text(e_product.get())
            color = normalize_text(e_color.get())
            size = normalize_text(e_size.get())
            qty = normalize_text(e_qty.get()) or "1"
            if not product:
                continue
            results.append(ManualRow(product=product, color=color, size=size, qty=qty))
        if results:
            self.result = results
            self.win.destroy()
        else:
            self.result = None
            self.win.destroy()

    def on_cancel(self) -> None:
        self.result = None
        self.win.destroy()


def prompt_manual_rule(title: str, info_lines: List[str]) -> Optional[List[ManualRow]]:
    if not MANUAL_PROMPT_ENABLED:
        return None
    if tk is None:
        return None
    if sys.platform not in ("win32", "darwin") and not os.environ.get("DISPLAY"):
        return None
    try:
        root = tk.Tk()
        root.withdraw()
        dialog = ManualRuleDialog(root, title=title, info_lines=info_lines)
        root.wait_window(dialog.win)
        root.destroy()
        return dialog.result
    except Exception:
        return None


# =========================
# 매핑 테이블 (JSON에서 로딩)
# =========================

SELF_PREFIX_MAP: Dict[str, List[str]] = {}
SELF_SET_PRODUCT_MAP: Dict[str, str] = {}
SELF_NOTE_MAP: Dict[str, str] = {}
SMART_OPTION_PREFIX_MAP: Dict[str, str] = {}
SMART_PRODUCT_PREFIX_MAP: Dict[str, List[str]] = {}


def _is_list_of_str(value) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


def _is_dict_str_list(value) -> bool:
    return isinstance(value, dict) and all(isinstance(k, str) and _is_list_of_str(v) for k, v in value.items())


def _is_dict_str_str(value) -> bool:
    return isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items())


def _mapping_schema() -> Dict[str, str]:
    return {
        "SELF_PREFIX_MAP": "dict[str, list[str]]",
        "SELF_SET_PRODUCT_MAP": "dict[str, str]",
        "SELF_NOTE_MAP": "dict[str, str]",
        "SMART_OPTION_PREFIX_MAP": "dict[str, str]",
        "SMART_PRODUCT_PREFIX_MAP": "dict[str, list[str]]",
    }


def _validate_mapping_payload(payload: dict) -> List[str]:
    errors: List[str] = []
    schema = _mapping_schema()

    for key, expected in schema.items():
        if key not in payload:
            errors.append(f"필수 키 누락: {key}")
            continue
        value = payload[key]
        if expected == "dict[str, list[str]]":
            if not _is_dict_str_list(value):
                errors.append(f"{key} 타입 오류: {expected} 이어야 합니다.")
            elif len(value) == 0:
                errors.append(f"{key}가 비어 있습니다.")
        elif expected == "dict[str, str]":
            if not _is_dict_str_str(value):
                errors.append(f"{key} 타입 오류: {expected} 이어야 합니다.")
            elif len(value) == 0:
                errors.append(f"{key}가 비어 있습니다.")

    return errors


def load_mappings() -> None:
    """configs/excel_auto_mapping.json에서 매핑 테이블을 필수 로드한다."""
    global SELF_PREFIX_MAP, SELF_SET_PRODUCT_MAP, SELF_NOTE_MAP
    global SMART_OPTION_PREFIX_MAP, SMART_PRODUCT_PREFIX_MAP

    config_path = _resolve_config_path(os.path.join("configs", "excel_auto_mapping.json"))
    if config_path is None:
        msg = (
            "매핑 파일을 찾을 수 없습니다.\n"
            "exe와 같은 폴더(또는 _internal/configs/) 에\n"
            "excel_auto_mapping.json 이 있어야 합니다."
        )
        logger.error(msg)
        if tk is not None:
            try:
                _root = tk.Tk()
                _root.withdraw()
                from tkinter import messagebox
                messagebox.showerror("매핑 파일 오류", msg)
                _root.destroy()
            except Exception:
                pass
        sys.exit(1)

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        msg = f"매핑 파일 읽기 실패: {config_path}\n{e}"
        logger.error(msg)
        sys.exit(1)

    if not isinstance(payload, dict):
        logger.error("매핑 파일 형식 오류: 최상위가 dict여야 합니다.")
        sys.exit(1)

    validation_errors = _validate_mapping_payload(payload)
    if validation_errors:
        logger.error("매핑 파일 검증 실패:")
        for err in validation_errors:
            logger.error(f" - {err}")
        sys.exit(1)

    SELF_PREFIX_MAP = payload["SELF_PREFIX_MAP"]
    SELF_SET_PRODUCT_MAP = payload["SELF_SET_PRODUCT_MAP"]
    SELF_NOTE_MAP = payload["SELF_NOTE_MAP"]
    SMART_OPTION_PREFIX_MAP = payload["SMART_OPTION_PREFIX_MAP"]
    SMART_PRODUCT_PREFIX_MAP = payload["SMART_PRODUCT_PREFIX_MAP"]


load_mappings()


# =========================
# 자사몰 처리
# =========================


def find_self_prefixes(product_name: str) -> Optional[List[str]]:
    def compact(v: str) -> str:
        t = normalize_text(v)
        t = re.sub(r"\s+", "", t)
        return t

    if product_name in SELF_PREFIX_MAP:
        return SELF_PREFIX_MAP[product_name]
    product_compact = compact(product_name)
    if product_compact:
        for key, prefixes in SELF_PREFIX_MAP.items():
            if compact(key) == product_compact:
                return prefixes
    # 접두어 매칭 ("..._" 형태)
    for key, prefixes in SELF_PREFIX_MAP.items():
        if key.endswith("_") and product_name.startswith(key):
            return prefixes
        if key.endswith("_"):
            key_compact = compact(key)
            if product_compact.startswith(key_compact):
                return prefixes
    return None


def find_self_note(product_name: str) -> str:
    if product_name in SELF_NOTE_MAP:
        return SELF_NOTE_MAP[product_name]
    if "3PACK" in product_name and product_name.startswith("에어젤핏젤리B_"):
        return SELF_NOTE_MAP.get("에어젤핏젤리B_3PACK", "")
    if "5PACK" in product_name and product_name.startswith("에어젤핏젤리B_"):
        return SELF_NOTE_MAP.get("에어젤핏젤리B_5PACK", "")
    if "3PACK" in product_name and product_name.startswith("에어후크젤리B_"):
        return SELF_NOTE_MAP.get("에어후크젤리B_3PACK", "")
    if "5PACK" in product_name and product_name.startswith("에어후크젤리B_"):
        return SELF_NOTE_MAP.get("에어후크젤리B_5PACK", "")
    if "3PACK" in product_name and product_name.startswith("캐미젤리B_"):
        return SELF_NOTE_MAP.get("캐미젤리B_3PACK", "")
    if "5PACK" in product_name and product_name.startswith("캐미젤리B_"):
        return SELF_NOTE_MAP.get("캐미젤리B_5PACK", "")
    return ""


def clean_option_product_name(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r"^선택\s*\d*\.?\s*", "", name)
    name = re.sub(r"\.+\s*\d+$", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_option_text_for_parsing(text: str) -> str:
    t = normalize_text(text)
    if not t:
        return ""

    t = t.replace("／", "/")
    # 상품명. 1{...} 형태를 선택 1. 상품명{...} 형태로 정규화한다.
    t = re.sub(
        r"([A-Za-z0-9가-힣_\-\+\[\]\(\) ]+)\.\s*(\d+)\s*\{",
        lambda m: f"선택 {int(m.group(2))}. {normalize_text(m.group(1))}{{",
        t,
    )
    # 선택 표기 변형(선택1, 선택 1, 선택 1:, 선택 1=)을 통일한다.
    t = re.sub(r"선택\s*(\d+)\s*[\.:=]?\s*", lambda m: f"선택 {int(m.group(1))}. ", t)
    # 사은품 표기 변형 통일
    t = t.replace("사은품 증정", "사은품")
    t = t.replace("사은품.", "사은품 ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def tokenize_option_chunks(option_text: str) -> List[str]:
    t = normalize_option_text_for_parsing(option_text)
    if not t:
        return []
    if "//" in t:
        parts = [p.strip() for p in t.split("//") if p.strip()]
    else:
        parts = [p.strip() for p in re.split(r"/|;", t) if p.strip()]
    return parts


def parse_self_options(full_name: str) -> List[Tuple[str, str, str]]:
    full_name = normalize_option_text_for_parsing(full_name)
    if is_prefixed_sku(full_name):
        prefix, color, size = split_prefixed_sku(full_name)
        return [(prefix, color, size)]

    if "(" in full_name and ")" in full_name:
        inside = full_name[full_name.find("(") + 1: full_name.rfind(")")]
    else:
        # Cafe24 실무 양식은 괄호 없이 "선택 1=...; 선택 2=..." 형태를 사용한다.
        inside = full_name

    if "색상=" in inside and "사이즈=" in inside:
        color = re.search(r"색상\s*=\s*([^,;/\)]+)", inside)
        size = re.search(r"사이즈\s*=\s*([^,;/\)]+)", inside)
        color_val = normalize_text(color.group(1)) if color else ""
        size_val = normalize_text(size.group(1)) if size else ""
        return [("", color_val, size_val)]

    if not inside:
        return []

    # 세트/팩 형태: 쉼표/세미콜론/더블슬래시 혼합 분리를 허용한다.
    parts = [p.strip() for p in re.split(r",|;|//", inside) if p.strip()]
    results: List[Tuple[str, str, str]] = []
    for part in parts:
        part = re.sub(r"^선택\s*\d+\s*[\.:= ]*", "", part).strip()
        if not part:
            continue
        left = ""
        right = ""
        if "=" in part:
            left, right = part.split("=", 1)
        elif ":" in part:
            left, right = part.split(":", 1)
        elif "{" in part and "}" in part:
            # 템플릿 후보 목록은 주문값이 아니므로 직접 분해 대상에서 제외한다.
            continue
        else:
            # "선택 1. 블랙 M"처럼 상품명이 생략된 PACK 옵션도 처리한다.
            if "사은품" in part:
                continue
            color, size = split_color_size(part)
            if color:
                results.append(("", color, size))
            continue
        # 사은품 항목은 별도 gift 규칙으로 처리한다.
        if "사은품" in left:
            continue
        product = clean_option_product_name(left)
        product = product.replace(" ", "")
        product = product.replace(".", "")
        prefix = SELF_SET_PRODUCT_MAP.get(product, "")
        color, size = split_color_size(right)
        results.append((prefix, color, size))
    return results


def derive_selected_size(full_name: str, parsed: List[Tuple[str, str, str]]) -> str:
    for _, _, size in parsed:
        size = normalize_text(size)
        if size and size.upper() != "FREE":
            return size
    m = re.search(r"사이즈\s*=\s*([^,\)]+)", normalize_text(full_name))
    if m:
        return normalize_text(m.group(1))
    return "FREE"


def _split_option_tokens(store: str, option_text: str) -> List[str]:
    t = normalize_option_text_for_parsing(option_text)
    if not t:
        return []
    if store == "자사몰":
        if "(" in t and ")" in t:
            inside = t[t.find("(") + 1: t.rfind(")")]
            return [p.strip() for p in re.split(r",|//", inside) if p.strip()]
        return tokenize_option_chunks(t)
    return tokenize_option_chunks(t)


def normalize_gift_match_text(text: str) -> str:
    t = normalize_text(text)
    t = re.sub(r"^선택\s*\d*\.?\s*", "", t)
    t = t.replace("사은품.", "사은품")
    t = t.replace("사은품 :", "사은품:")
    t = re.sub(r"\s+", "", t)
    return t


def _rebuild_option_text(store: str, original: str, kept_tokens: List[str]) -> str:
    o = normalize_text(original)
    if store == "자사몰":
        if "(" in o:
            head = o[:o.find("(")].strip()
            if kept_tokens:
                return f"{head}({', '.join(kept_tokens)})"
            return head
        return o
    return " / ".join(kept_tokens)


def extract_gift_events(
    store: str,
    product: str,
    option_text: str,
    gift_store: GiftRuleStore,
) -> Tuple[List[Dict[str, Any]], str, List[str]]:
    events: List[Dict[str, Any]] = []
    unknown_gift_tokens: List[str] = []
    rules = gift_store.get_rules()

    product_norm = normalize_text(product)
    product_cmp = normalize_gift_match_text(product_norm)
    tokens = _split_option_tokens(store, option_text)
    kept_tokens: List[str] = []

    # 1) 주문상품명 기준 증정 규칙
    for rule in rules:
        if normalize_text(rule.get("match_field")) != "product":
            continue
        r_store = normalize_text(rule.get("store")) or "공통"
        if r_store not in ("공통", store):
            continue
        needle = normalize_text(rule.get("contains"))
        needle_cmp = normalize_gift_match_text(needle)
        if needle_cmp and needle_cmp in product_cmp:
            events.append({"rule": rule, "source": product_norm})

    # 2) 옵션 문자열 기준 증정 규칙 (가장 먼저 분리)
    for tok in tokens:
        matched = False
        tok_cmp = normalize_gift_match_text(tok)
        tok_display = re.sub(r"^선택\s*\d*\.?\s*", "", normalize_text(tok))
        for rule in rules:
            if normalize_text(rule.get("match_field")) != "option":
                continue
            r_store = normalize_text(rule.get("store")) or "공통"
            if r_store not in ("공통", store):
                continue
            needle = normalize_text(rule.get("contains"))
            needle_cmp = normalize_gift_match_text(needle)
            if needle_cmp and needle_cmp in tok_cmp:
                events.append({"rule": rule, "source": tok_display})
                matched = True
                break
        if matched:
            continue
        if "사은품" in tok_cmp:
            unknown_gift_tokens.append(tok_display)
            continue
        kept_tokens.append(tok)

    cleaned = _rebuild_option_text(store, option_text, kept_tokens)
    return events, cleaned, unknown_gift_tokens


def build_gift_rows_from_events(
    events: List[Dict[str, Any]],
    selected_size: str,
    base_row: Dict[str, str],
) -> List[Dict[str, str]]:
    out_rows: List[Dict[str, str]] = []
    seen: set = set()
    chosen_size = normalize_text(selected_size) or "FREE"
    for ev in events:
        rule = ev.get("rule", {})
        output = rule.get("output", {})
        if not isinstance(output, dict):
            continue
        p = normalize_text(output.get("product"))
        c = normalize_text(output.get("color"))
        size_mode = normalize_text(output.get("size_mode"))
        size_value = normalize_text(output.get("size_value"))
        size_suffix = normalize_text(output.get("size_suffix"))
        qty = normalize_text(output.get("qty")) or "1"
        if size_mode == "fixed":
            s = size_value or "FREE(증정)"
        else:
            s = f"{chosen_size}{size_suffix}".strip() if size_suffix else chosen_size
        key = (p, c, s, qty)
        if key in seen:
            continue
        seen.add(key)
        row = dict(base_row)
        row["출고명"] = f"{p}_{c}_{s}".strip("_")
        row["수량"] = int(float(qty)) if str(qty).isdigit() else qty
        out_rows.append(row)
    return out_rows


def build_self_rows(
    df: pd.DataFrame,
    rule_store: CustomRuleStore,
    gift_store: GiftRuleStore,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    rows: List[Dict[str, str]] = []
    skipped_rows: List[Dict[str, str]] = []
    for _, r in df.iterrows():
        order_no = normalize_text(r.get("주문번호"))
        if not order_no:
            continue
        product = normalize_text(r.get("주문상품명"))
        full_name = normalize_text(r.get("주문상품명(옵션포함)"))
        qty = normalize_text(r.get("수량")) or "1"
        gift_events, full_name_clean, unknown_gifts = extract_gift_events("자사몰", product, full_name, gift_store)

        base_row = {
            "주문번호": order_no,
            "이름": normalize_text(r.get("수령인")),
            "주소": build_self_address(r),
            "핸드폰": normalize_text(r.get("수령인 휴대전화")) or normalize_text(r.get("핸드폰")) or normalize_text(r.get("전화번호")),
            "배송메시지": normalize_delivery_message(r.get("배송메시지")),
            "출고명": "",
            "수량": "",
            "비고": find_self_note(product),
            "행 레이블": "",
            "수령인": "",
            "주소.1": "",
            "수령인 휴대전화": "",
            "비고.1": "",
        }

        for tok in unknown_gifts:
            skipped_rows.append({
                "주문번호": order_no,
                "주문상품명": product,
                "주문상품명(옵션포함)": full_name,
                "수량": "1",
                "누락사유": f"사은품 규칙 미지원: {tok}",
            })

        parsed_preview = parse_self_options(full_name_clean)

        def append_main_row(prefix: str, color: str, size: str, row_qty: str) -> None:
            out = dict(base_row)
            out["출고명"] = f"{prefix}_{color}_{size}".strip("_")
            out["수량"] = int(float(row_qty)) if str(row_qty).isdigit() else row_qty
            rows.append(out)

        def append_gift_rows(selected_size: str) -> None:
            rows.extend(build_gift_rows_from_events(gift_events, selected_size, base_row))

        custom_key = f"스토어타입=자사몰|{product}|{full_name_clean}"
        custom_rule = rule_store.get(custom_key)
        if custom_rule:
            selected_size = derive_selected_size(full_name_clean, [])
            for item in custom_rule:
                if is_gift_like_rule_item(item):
                    logger.info("[커스텀규칙] 사은품 항목 무시(자사몰): %s", custom_key)
                    continue
                prefix = normalize_text(item.get("product"))
                color = normalize_text(item.get("color"))
                size = normalize_text(item.get("size"))
                item_qty = normalize_text(item.get("qty")) or qty
                if size and size.upper() != "FREE" and "(증정)" not in size:
                    selected_size = size
                append_main_row(prefix, color, size, item_qty)
            append_gift_rows(selected_size)
            continue

        parsed = parsed_preview
        if not parsed:
            # 수동 입력
            manual = prompt_manual_rule(
                title="이 데이터는 어떻게 분해해야 합니까?",
                info_lines=[
                    f"주문상품명: {product}",
                    f"주문상품명(옵션포함): {full_name_clean}",
                    f"수량: {qty}",
                    "세트가 아닌 경우 수량은 무조건 1로 넣어주세요.",
                    "3pack 처럼 여러개 들어있는 제품만 해당 수량을 넣어주시면 됩니다.",
                ],
            )
            if manual:
                to_save = []
                selected_size = derive_selected_size(full_name_clean, [])
                for m in manual:
                    to_save.append({
                        "product": m.product,
                        "color": m.color,
                        "size": m.size,
                        "qty": m.qty,
                    })
                    if m.size and m.size.upper() != "FREE" and "(증정)" not in m.size:
                        selected_size = m.size
                    append_main_row(m.product, m.color, m.size, m.qty)
                rule_store.set(custom_key, to_save)
                append_gift_rows(selected_size)
            else:
                logger.warning(f"[자사몰] 분해 실패(수동입력 없음): {product} / {full_name_clean}")
                skipped_rows.append({
                    "주문번호": order_no,
                    "주문상품명": product,
                    "주문상품명(옵션포함)": full_name_clean,
                    "수량": qty,
                    "누락사유": "분해 실패(수동입력 없음)",
                })
                append_gift_rows(derive_selected_size(full_name_clean, []))
            continue

        prefixes = find_self_prefixes(product)
        selected_size = derive_selected_size(full_name_clean, parsed)

        # 단품 (prefix 미지정이면 주문상품명 매핑 사용)
        if len(parsed) == 1 and parsed[0][0] == "":
            color, size = parsed[0][1], parsed[0][2]
            prefix = prefixes[0] if prefixes else ""
            if not prefix:
                logger.warning(f"[자사몰] prefix 누락(단품): {product} / {full_name_clean}")
                skipped_rows.append({
                    "주문번호": order_no,
                    "주문상품명": product,
                    "주문상품명(옵션포함)": full_name_clean,
                    "수량": qty,
                    "누락사유": f"prefix 매핑 없음 (SELF_PREFIX_MAP에 '{product}' 없음)",
                })
                append_gift_rows(selected_size)
                continue
            append_main_row(prefix, color, size, qty)
            append_gift_rows(selected_size)
            continue

        # 세트/팩
        generated_any = False
        for prefix, color, size in parsed:
            actual_prefix = prefix
            if not actual_prefix and prefixes:
                actual_prefix = prefixes[0]
            if not actual_prefix:
                continue
            generated_any = True
            append_main_row(actual_prefix, color, size, qty)
        if not generated_any:
            logger.warning(f"[자사몰] prefix 누락(세트): {product} / {full_name_clean}")
            skipped_rows.append({
                "주문번호": order_no,
                "주문상품명": product,
                "주문상품명(옵션포함)": full_name_clean,
                "수량": qty,
                "누락사유": f"prefix 매핑 없음 (SELF_PREFIX_MAP에 '{product}' 없음)",
            })
            append_gift_rows(selected_size)
            continue
        append_gift_rows(selected_size)
    return rows, skipped_rows


def build_self_address(row: pd.Series) -> str:
    base = normalize_text(row.get("수령인 주소"))
    detail = normalize_text(row.get("수령인 상세 주소"))
    if not base:
        base = normalize_text(row.get("주소"))
    if base and detail:
        return f"{base} {detail}"
    return base or detail


# =========================
# 스마트스토어 처리
# =========================


def find_smart_prefix(product_name: str) -> Optional[List[str]]:
    product_name = normalize_text(product_name)

    if product_name in SMART_PRODUCT_PREFIX_MAP:
        return SMART_PRODUCT_PREFIX_MAP[product_name]

    compact = re.sub(r"\s+", "", product_name).lower()
    for key, prefixes in SMART_PRODUCT_PREFIX_MAP.items():
        key_compact = re.sub(r"\s+", "", normalize_text(key)).lower()
        if key_compact and key_compact == compact:
            return prefixes

    for key, prefixes in SMART_PRODUCT_PREFIX_MAP.items():
        if key.endswith("_") and product_name.startswith(key):
            return prefixes
        key_compact = re.sub(r"\s+", "", normalize_text(key)).lower()
        if key.endswith("_") and compact.startswith(key_compact):
            return prefixes

    # 라벨/브랜드/태그 순서가 다른 스마트스토어 상품명 보정
    normalized = re.sub(r"\[[^\]]+\]", " ", product_name)
    normalized = re.sub(r"\b(타밈|tamim|심리스|노와이어|얇은끈)\b", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    ncmp = re.sub(r"\s+", "", normalized).lower()
    if "캐미젤리브라" in ncmp or ("캐미" in ncmp and "젤리브라" in ncmp):
        return ["[B]캐미젤리B"]
    if "에어젤핏젤리브라" in ncmp:
        return ["에어젤핏젤리B"]
    if "에어후크젤리브라" in ncmp:
        return ["에어후크젤리B"]
    if "로크젤리브라" in ncmp:
        return ["로크젤리B"]

    return None


def parse_smart_options(option_info: str) -> List[Tuple[str, str, str, bool]]:
    option_info = normalize_option_text_for_parsing(option_info)
    if not option_info:
        return []

    parts = tokenize_option_chunks(option_info)
    results: List[Tuple[str, str, str, bool]] = []
    color_buffer = ""
    size_buffer = ""

    for part in parts:
        raw_part = part
        part = re.sub(r"^선택\s*\d+\s*[\.:= ]*", "", part).strip()
        part = re.sub(r"^사은품\s*[:=]?\s*", "", part).strip()
        if not part:
            continue

        is_gift = ("사은품" in raw_part) or ("사은품" in part)
        if part.startswith("컬러") or part.startswith("색상"):
            if ":" in part:
                _, val = part.split(":", 1)
            elif "=" in part:
                _, val = part.split("=", 1)
            else:
                continue
            color_buffer = normalize_text(val)
            continue
        if part.startswith("사이즈"):
            if ":" in part:
                _, val = part.split(":", 1)
            elif "=" in part:
                _, val = part.split("=", 1)
            else:
                continue
            size_buffer = normalize_text(val)
            continue

        if ":" in part or "=" in part:
            opt_name, opt_val = re.split(r"[:=]", part, maxsplit=1)
            opt_name = normalize_text(opt_name)
            opt_val = normalize_text(opt_val)
            if is_prefixed_sku(opt_val):
                prefix, color, size = split_prefixed_sku(opt_val)
                results.append((prefix, color, size, is_gift))
                continue
            color, size = split_color_size(opt_val)
            if is_gift and opt_val:
                results.append((opt_val, color, size, True))
            else:
                results.append((opt_name, color, size, is_gift))
            continue

        if "{" in part and "}" in part:
            # 템플릿 후보 목록 문자열은 실제 주문값이 아니므로 파싱 대상에서 제외한다.
            continue

        # 단독 값
        if is_prefixed_sku(part):
            prefix, color, size = split_prefixed_sku(part)
            results.append((prefix, color, size, is_gift))
            continue

        color, size = split_color_size(part)
        results.append(("", color, size, is_gift))

    # 색상/사이즈 버퍼는 독립 옵션이 전혀 없을 때만 사용한다.
    if (color_buffer or size_buffer) and not results:
        results.append(("", color_buffer, size_buffer or "FREE", False))

    return results


def build_smart_rows(
    df: pd.DataFrame,
    rule_store: CustomRuleStore,
    gift_store: GiftRuleStore,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    rows: List[Dict[str, str]] = []
    skipped_rows: List[Dict[str, str]] = []
    smooth_free_pack_colors = [
        "내추럴베이지",
        "멜란지화이트",
        "블랙",
        "코코아베이지",
        "파우더핑크",
    ]
    for _, r in df.iterrows():
        status = normalize_text(r.get("주문상태"))
        if status in {"취소", "반품", "환불"}:
            continue
        order_no = normalize_text(r.get("상품주문번호"))
        if not order_no:
            continue

        product = normalize_text(r.get("상품명"))
        option_info = normalize_text(r.get("옵션정보"))
        qty = normalize_text(r.get("수량")) or "1"
        gift_events, option_info_clean, unknown_gifts = extract_gift_events("스마트스토어", product, option_info, gift_store)

        for tok in unknown_gifts:
            skipped_rows.append({
                "상품주문번호": order_no,
                "상품명": product,
                "옵션정보": option_info,
                "수량": "1",
                "누락사유": f"사은품 규칙 미지원: {tok}",
            })

        def selected_size_from_rows(parsed_rows: List[Tuple[str, str, str, bool]]) -> str:
            for _, _, s, _ in parsed_rows:
                s = normalize_text(s)
                if s and s.upper() != "FREE" and "(증정)" not in s:
                    return s
            m = re.findall(r"\b(XXL|XL|L|M|S|XS|FREE)\b", option_info_clean, flags=re.IGNORECASE)
            if m:
                return m[-1].upper()
            return "FREE"

        def append_gift_rows(selected_size: str) -> None:
            base_row = build_smart_row(r, order_no, "", "1")
            rows.extend(build_gift_rows_from_events(gift_events, selected_size, base_row))

        custom_key = f"스토어타입=스마트스토어|{product}|{option_info_clean}"
        custom_rule = rule_store.get(custom_key)
        if custom_rule:
            selected_size = "FREE"
            for item in custom_rule:
                if is_gift_like_rule_item(item):
                    logger.info("[커스텀규칙] 사은품 항목 무시(스마트스토어): %s", custom_key)
                    continue
                prefix = normalize_text(item.get("product"))
                color = normalize_text(item.get("color"))
                size = normalize_text(item.get("size"))
                item_qty = normalize_text(item.get("qty")) or qty
                if size and size.upper() != "FREE" and "(증정)" not in size:
                    selected_size = size
                output_name = f"{prefix}_{color}_{size}".strip("_")
                rows.append(build_smart_row(r, order_no, output_name, item_qty))
            append_gift_rows(selected_size)
            continue

        parsed = parse_smart_options(option_info_clean)
        if not parsed:
            manual = prompt_manual_rule(
                title="이 데이터는 어떻게 분해해야 합니까?",
                info_lines=[
                    f"상품명: {product}",
                    f"옵션정보: {option_info_clean}",
                    f"수량: {qty}",
                    "세트가 아닌 경우 수량은 무조건 1로 넣어주세요.",
                    "3pack 처럼 여러개 들어있는 제품만 해당 수량을 넣어주시면 됩니다.",
                ],
            )
            if manual:
                to_save = []
                selected_size = "FREE"
                for m in manual:
                    to_save.append({
                        "product": m.product,
                        "color": m.color,
                        "size": m.size,
                        "qty": m.qty,
                    })
                    if m.size and m.size.upper() != "FREE" and "(증정)" not in m.size:
                        selected_size = m.size
                    output_name = f"{m.product}_{m.color}_{m.size}".strip("_")
                    rows.append(build_smart_row(r, order_no, output_name, m.qty))
                rule_store.set(custom_key, to_save)
                append_gift_rows(selected_size)
            else:
                logger.warning(f"[스마트스토어] 분해 실패(수동입력 없음): {product} / {option_info_clean}")
                skipped_rows.append({
                    "상품주문번호": order_no,
                    "상품명": product,
                    "옵션정보": option_info_clean,
                    "수량": qty,
                    "누락사유": "분해 실패(수동입력 없음)",
                })
                append_gift_rows("FREE")
            continue

        prefixes = find_smart_prefix(product) or []
        selected_size = selected_size_from_rows(parsed)

        # 특수 케이스: 스무스 프리 브리프 5P/5PACK은 5개 컬러로 강제 분해한다.
        if "스무스 프리 브리프" in product and ("5P" in product or "5PACK" in product):
            size = "FREE"
            for _, _, s, _ in parsed:
                if s:
                    size = s
                    break

            for color in smooth_free_pack_colors:
                output_name = f"스무스프리P_{color}_{size}".strip("_")
                rows.append(build_smart_row(r, order_no, output_name, "1"))

            append_gift_rows(selected_size)
            continue

        # 특수 케이스: 미드나잇 세트
        if "미드나잇 세트" in product and prefixes:
            # 컬러/사이즈는 옵션정보에서 추출
            color = ""
            size = "FREE"
            for opt_name, c, s, _ in parsed:
                if c:
                    color = c
                if s:
                    size = s
            for p in prefixes:
                output_name = f"{p}_{color}_{size}".strip("_")
                rows.append(build_smart_row(r, order_no, output_name, qty))
            append_gift_rows(selected_size)
            continue

        is_magic = "매직스타킹" in product and "3PACK" in product
        magic_main_done = False

        for opt_name, color, size, is_gift in parsed:
            # 매직스타킹 3PACK 고정
            if is_magic:
                if not magic_main_done:
                    output_name = "매직스타킹_블랙_FREE(ver2)"
                    rows.append(build_smart_row(r, order_no, output_name, qty))
                    magic_main_done = True
                continue

            # 옵션명이 있으면 옵션명 기준 매핑
            prefix = ""
            if opt_name:
                prefix = SMART_OPTION_PREFIX_MAP.get(opt_name, opt_name)

            # 옵션명이 없으면 상품명 매핑
            if not prefix:
                prefix = prefixes[0] if prefixes else ""

            # 다중 prefix인데 옵션명이 없으면 동일 옵션을 모두 생성
            if not opt_name and len(prefixes) > 1:
                for p in prefixes:
                    output_name = f"{p}_{color}_{size}".strip("_")
                    rows.append(build_smart_row(r, order_no, output_name, qty))
                continue

            output_name = f"{prefix}_{color}_{size}".strip("_")
            rows.append(build_smart_row(r, order_no, output_name, qty))
        append_gift_rows(selected_size)

    return rows, skipped_rows


def build_smart_row(r: pd.Series, order_no: str, output_name: str, qty: str) -> Dict[str, str]:
    return {
        "주문번호": order_no,
        "이름": normalize_text(r.get("수취인명")) or normalize_text(r.get("구매자명")),
        "주소": build_smart_address(r),
        "핸드폰": normalize_text(r.get("수취인연락처1")) or normalize_text(r.get("구매자연락처")),
        "배송메세지": normalize_delivery_message(r.get("배송메세지")),
        "출고명": output_name,
        "수량": int(float(qty)) if str(qty).isdigit() else qty,
        "행 레이블": "",
        "수취인명": "",
        "통합배송지": "",
        "수취인연락처1": "",
    }


def build_smart_address(row: pd.Series) -> str:
    base = normalize_text(row.get("기본배송지"))
    detail = normalize_text(row.get("상세배송지"))
    if base or detail:
        return f"{base} {detail}".strip()
    return normalize_text(row.get("통합배송지"))


# =========================
# 후처리 공통
# =========================


def aggregate_and_finalize_self(rows: List[Dict[str, str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "주문번호", "이름", "주소", "핸드폰", "배송메시지", "출고명", "수량", "비고",
            "행 레이블", "수령인", "주소.1", "수령인 휴대전화", "비고.1",
        ])
    df = pd.DataFrame(rows)
    df["수량"] = pd.to_numeric(df["수량"], errors="coerce").fillna(1).astype(int)
    df = df.groupby(["주문번호", "출고명"], as_index=False).agg({
        "이름": "first",
        "주소": "first",
        "핸드폰": "first",
        "배송메시지": "first",
        "수량": "sum",
        "비고": "first",
    })

    df = df.sort_values(["주문번호", "출고명"], ascending=[True, True]).reset_index(drop=True)

    df["행 레이블"] = ""
    df["수령인"] = ""
    df["주소.1"] = ""
    df["수령인 휴대전화"] = ""
    df["비고.1"] = ""

    for order_no, idxs in df.groupby("주문번호").groups.items():
        first_idx = min(idxs)
        msg_vals = [normalize_text(df.loc[i, "배송메시지"]) for i in idxs]
        first_msg = next((m for m in msg_vals if m), "")
        df.loc[list(idxs), "배송메시지"] = ""
        if first_msg:
            df.loc[first_idx, "배송메시지"] = first_msg
        df.loc[first_idx, "행 레이블"] = order_no
        df.loc[first_idx, "수령인"] = df.loc[first_idx, "이름"]
        df.loc[first_idx, "주소.1"] = df.loc[first_idx, "주소"]
        df.loc[first_idx, "수령인 휴대전화"] = df.loc[first_idx, "핸드폰"]
        df.loc[first_idx, "비고.1"] = df.loc[first_idx, "비고"]

    df = df[[
        "주문번호", "이름", "주소", "핸드폰", "배송메시지", "출고명", "수량", "비고",
        "행 레이블", "수령인", "주소.1", "수령인 휴대전화", "비고.1",
    ]]
    return df


def aggregate_and_finalize_smart(rows: List[Dict[str, str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "주문번호", "이름", "주소", "핸드폰", "배송메세지", "출고명", "수량",
            "행 레이블", "수취인명", "통합배송지", "수취인연락처1",
        ])
    df = pd.DataFrame(rows)
    df["수량"] = pd.to_numeric(df["수량"], errors="coerce").fillna(1).astype(int)
    df = df.groupby(["주문번호", "출고명"], as_index=False).agg({
        "이름": "first",
        "주소": "first",
        "핸드폰": "first",
        "배송메세지": "first",
        "수량": "sum",
    })

    df = df.sort_values(["주문번호", "출고명"], ascending=[True, True]).reset_index(drop=True)

    df["행 레이블"] = ""
    df["수취인명"] = ""
    df["통합배송지"] = ""
    df["수취인연락처1"] = ""

    for order_no, idxs in df.groupby("주문번호").groups.items():
        first_idx = min(idxs)
        msg_vals = [normalize_text(df.loc[i, "배송메세지"]) for i in idxs]
        first_msg = next((m for m in msg_vals if m), "")
        df.loc[list(idxs), "배송메세지"] = ""
        if first_msg:
            df.loc[first_idx, "배송메세지"] = first_msg
        df.loc[first_idx, "행 레이블"] = order_no
        df.loc[first_idx, "수취인명"] = df.loc[first_idx, "이름"]
        df.loc[first_idx, "통합배송지"] = df.loc[first_idx, "주소"]
        df.loc[first_idx, "수취인연락처1"] = df.loc[first_idx, "핸드폰"]

    df = df[[
        "주문번호", "이름", "주소", "핸드폰", "배송메세지", "출고명", "수량",
        "행 레이블", "수취인명", "통합배송지", "수취인연락처1",
    ]]
    return df


# =========================
# 메인 실행
# =========================


def process_file(
    path: Path,
    output_dir: Path,
    rule_store: CustomRuleStore,
    gift_store: GiftRuleStore,
) -> Tuple[Path, Optional[Path]]:
    """파일을 처리하고 (변환파일경로, 누락파일경로|None) 튜플을 반환한다."""
    logger.info(f"파일 처리 시작: {path.name}")
    # 헤더 판별 (스마트스토어/자사몰)
    header_idx = detect_header_row(path, "상품주문번호")
    df = pd.read_excel(path, sheet_name=0, header=header_idx)

    skipped_rows: List[Dict[str, str]] = []

    if "상품주문번호" in df.columns:
        original_df = df
        rows, skipped_rows = build_smart_rows(df, rule_store, gift_store)
        result = aggregate_and_finalize_smart(rows)
    else:
        header_idx = detect_header_row(path, "주문번호")
        df_raw = pd.read_excel(path, sheet_name=0, header=header_idx)
        df = normalize_cafe24_dataframe(df_raw)
        if "주문번호" in df.columns and ("주문상품명(옵션포함)" in df.columns or "옵션" in df_raw.columns):
            original_df = df_raw
            rows, skipped_rows = build_self_rows(df, rule_store, gift_store)
            result = aggregate_and_finalize_self(rows)
        else:
            raise ValueError(f"지원되지 않는 파일 형식: {path}")

    if "행 레이블" in result.columns:
        if "주문번호" in result.columns:
            result = result.rename(columns={"주문번호": "원주문번호"})
        result = result.rename(columns={"행 레이블": "주문번호"})

    # ID 계열 컬럼은 과학적 표기 없이 텍스트로 고정한다.
    original_df = enforce_id_text_columns(original_df)
    result = enforce_id_text_columns(result)

    output_name = f"{VERSION}_{path.stem}_변환.xlsx"
    output_path = output_dir / output_name
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="다운로드원본", index=False)
        result.to_excel(writer, sheet_name="변환완료", index=False)

    # 누락 파일 출력
    skipped_path: Optional[Path] = None
    if skipped_rows:
        skipped_df = pd.DataFrame(skipped_rows)
        skipped_df = enforce_id_text_columns(skipped_df)
        skipped_name = f"누락_{VERSION}_{path.stem}.xlsx"
        skipped_path = output_dir / skipped_name
        skipped_df.to_excel(skipped_path, index=False)
        logger.info(f"[누락] {len(skipped_rows)}건 → {skipped_path.name}")

    return output_path, skipped_path


def main():
    global MANUAL_PROMPT_ENABLED
    logger.info(f"=== excel_auto_ver{VERSION} 시작 ===")
    import subprocess
    MANUAL_PROMPT_ENABLED = True

    # CommandLineTools Python은 Tk 초기화 시 크래시가 발생할 수 있으므로 직접 사용하지 않음
    if "CommandLineTools" in sys.executable:
        gui_py = find_gui_python(subprocess)
        if gui_py:
            os.execv(gui_py, [gui_py, __file__, "--gui"])
        logger.error("GUI 환경을 사용할 수 없습니다. (Tk+Pandas 호환 Python 필요)")
        return
    if tk is None or (sys.platform not in ("win32", "darwin") and not os.environ.get("DISPLAY")):
        logger.error("GUI 환경을 사용할 수 없습니다.")
        return
    run_gui()


def open_folder_in_os(path: Path) -> None:
    target = str(path)
    try:
        if sys.platform == "darwin":
            import subprocess
            subprocess.run(["open", target], check=False)
        elif sys.platform.startswith("win"):
            os.startfile(target)  # type: ignore[attr-defined]
        else:
            import subprocess
            subprocess.run(["xdg-open", target], check=False)
    except Exception as e:
        logger.error(f"폴더 열기 실패: {target} / {e}")


def bind_global_edit_shortcuts(root) -> None:
    def do_select_all(widget):
        try:
            if isinstance(widget, tk.Entry):
                widget.selection_range(0, "end")
                widget.icursor("end")
                return "break"
            if isinstance(widget, tk.Text):
                widget.tag_add("sel", "1.0", "end-1c")
                widget.mark_set("insert", "end-1c")
                return "break"
        except Exception:
            return None
        return None

    def on_copy(_event):
        try:
            _event.widget.event_generate("<<Copy>>")
            return "break"
        except Exception:
            return None

    def on_paste(_event):
        try:
            _event.widget.event_generate("<<Paste>>")
            return "break"
        except Exception:
            return None

    def on_cut(_event):
        try:
            _event.widget.event_generate("<<Cut>>")
            return "break"
        except Exception:
            return None

    def on_select_all(_event):
        return do_select_all(_event.widget)

    # Control 조합은 플랫폼 공통 바인딩
    for mod in ("Control",):
        root.bind_all(f"<{mod}-c>", on_copy, add="+")
        root.bind_all(f"<{mod}-C>", on_copy, add="+")
        root.bind_all(f"<{mod}-v>", on_paste, add="+")
        root.bind_all(f"<{mod}-V>", on_paste, add="+")
        root.bind_all(f"<{mod}-x>", on_cut, add="+")
        root.bind_all(f"<{mod}-X>", on_cut, add="+")
        root.bind_all(f"<{mod}-a>", on_select_all, add="+")
        root.bind_all(f"<{mod}-A>", on_select_all, add="+")

    # macOS 한/영 상태에서 keysym이 영문자가 아닌 경우를 대비해 keycode도 보조 처리한다.
    def on_command_keypress(event):
        if sys.platform != "darwin":
            return None
        keycode_map = {
            8: on_copy,    # C
            9: on_paste,   # V
            7: on_cut,     # X
            0: on_select_all,  # A
        }
        fn = keycode_map.get(getattr(event, "keycode", -1))
        if fn:
            return fn(event)
        return None

    if sys.platform == "darwin":
        root.bind_all("<Command-KeyPress>", on_command_keypress, add="+")


def run_gui():
    root = create_gui_root()
    force_single_root(root)
    schedule_cleanup_extra_windows(root)
    root.title(f"excel_auto_ver{VERSION}")
    root.geometry("860x680")
    root.configure(bg="#f6f7f9")
    bind_global_edit_shortcuts(root)

    cafe24_var = tk.StringVar(master=root, value="")
    smart_var = tk.StringVar(master=root, value="")
    output_var = tk.StringVar(master=root, value=str(get_default_output_dir()))
    cafe24_status = tk.StringVar(master=root, value="파일을 로드하세요")
    smart_status = tk.StringVar(master=root, value="파일을 로드하세요")
    result_status = tk.StringVar(master=root, value="변환 완료 / 미완료")
    result_detail = tk.StringVar(master=root, value="변환 완료 0건, 분해 옵션 0건 추가됨")

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass
    default_font = ("Apple SD Gothic Neo", 12)
    root.option_add("*Font", default_font)

    def choose_cafe24():
        path = filedialog.askopenfilename(
            title="Cafe24(자사몰) 파일 선택",
            filetypes=[("Excel Files", "*.xlsx")],
            parent=root,
        )
        if path:
            cafe24_var.set(path)
            cafe24_status.set(f"파일 로드됨: {compress_filename_for_status(path)}")

    def clear_cafe24():
        cafe24_var.set("")
        cafe24_status.set("파일을 로드하세요")

    def choose_smartstore():
        path = filedialog.askopenfilename(
            title="스마트스토어 파일 선택",
            filetypes=[("Excel Files", "*.xlsx")],
            parent=root,
        )
        if path:
            smart_var.set(path)
            smart_status.set(f"파일 로드됨: {compress_filename_for_status(path)}")

    def clear_smartstore():
        smart_var.set("")
        smart_status.set("파일을 로드하세요")

    def choose_output():
        path = filedialog.askdirectory(title="출력 폴더 선택", parent=root)
        if path:
            output_var.set(path)
            root.update_idletasks()

    def open_output_folder():
        out_dir = Path(output_var.get())
        ensure_dir(out_dir)
        open_folder_in_os(out_dir)

    def run_online_patch_update():
        ok, msg = apply_online_patch_update()
        if ok:
            messagebox.showinfo("온라인 패치", msg, parent=root)
            result_status.set("패치 적용 완료")
            result_detail.set("최신 규칙이 반영되었습니다. 변환을 다시 실행해주세요.")
        else:
            messagebox.showerror("온라인 패치 실패", msg, parent=root)

    def run():
        output_dir = Path(output_var.get())
        ensure_dir(output_dir)

        rule_store = CustomRuleStore(_resolve_config_path(os.path.join("configs", "custom_rules.json")) or get_custom_rules_path())
        gift_store = GiftRuleStore(_resolve_config_path(os.path.join("configs", "gift_rules.json")) or get_gift_rules_path())
        files = []
        if cafe24_var.get():
            files.append(Path(cafe24_var.get()))
        if smart_var.get():
            files.append(Path(smart_var.get()))
        if not files:
            messagebox.showinfo("안내", "Cafe24 또는 스마트스토어 파일을 선택해주세요.")
            return

        ok_count = 0
        fail_count = 0
        skipped_count = 0
        error_lines: List[str] = []
        for path in files:
            try:
                out, skipped = process_file(path, output_dir, rule_store, gift_store)
                ok_count += 1
                if skipped:
                    # 누락 파일에서 건수 읽기
                    try:
                        sk_df = pd.read_excel(skipped)
                        skipped_count += len(sk_df)
                    except Exception:
                        skipped_count += 1
            except Exception as e:
                fail_count += 1
                error_lines.append(f"- {path.name}: {e}")
        status_msg = "변환 완료" if fail_count == 0 else "변환 완료 / 일부 실패"
        result_status.set(status_msg)
        detail_msg = f"변환 완료 {ok_count}건, 실패 {fail_count}건"
        if skipped_count > 0:
            detail_msg += f", 누락 {skipped_count}건 (별도 파일 출력됨)"
        result_detail.set(detail_msg)
        if error_lines:
            detail = "\n".join(error_lines[:10])
            if len(error_lines) > 10:
                detail += f"\n... 외 {len(error_lines) - 10}건"
            messagebox.showerror("변환 실패 상세", detail, parent=root)

    container = tk.Frame(root, bg="#f6f7f9")
    container.pack(fill="both", expand=True, padx=20, pady=20)

    title = tk.Label(
        container,
        text="엑셀 자동변환기",
        font=("Apple SD Gothic Neo", 16, "bold"),
        bg="#f6f7f9",
        anchor="w",
    )
    title.pack(fill="x", pady=(0, 16))

    panel = tk.Frame(container, bg="#f6f7f9")
    panel.pack(fill="x")
    panel.grid_columnconfigure(0, weight=1, uniform="file_btn")
    panel.grid_columnconfigure(1, weight=1, uniform="file_btn")
    panel.grid_rowconfigure(0, weight=1)

    cafe_logo = load_local_icon(root, get_app_dir() / "Cafe24_Logo.png", 260, 110)
    ss_logo = load_local_icon(root, get_app_dir() / "SS_Logo.png", 260, 110)
    root._img_refs = [img for img in (cafe_logo, ss_logo) if img is not None]

    ss_bg = "#2C3441"

    cafe_card = tk.Frame(panel, bg="#ffffff", relief="solid", bd=2)
    cafe_card.grid(row=0, column=0, padx=(10, 6), pady=4, sticky="nsew")
    cafe_card.grid_columnconfigure(0, weight=1)
    cafe_card.grid_rowconfigure(0, weight=1)
    cafe_card.grid_rowconfigure(1, weight=0)

    cafe_btn = tk.Button(
        cafe_card,
        text="자사몰 파일 불러오기",
        command=choose_cafe24,
        image=cafe_logo,
        compound="top",
        bg="#ffffff",
        fg="#222222",
        relief="flat",
        bd=0,
        activebackground="#ffffff",
        highlightthickness=0,
        padx=12,
        pady=12,
    )
    cafe_btn.grid(row=0, column=0, sticky="nsew")
    tk.Button(
        cafe_card,
        text="로딩취소",
        command=clear_cafe24,
        bg="#ffffff",
        fg="#333333",
        relief="flat",
        bd=0,
        highlightthickness=0,
    ).grid(row=1, column=0, pady=(0, 10))

    smart_card = tk.Frame(panel, bg=ss_bg, relief="solid", bd=2)
    smart_card.grid(row=0, column=1, padx=(6, 10), pady=4, sticky="nsew")
    smart_card.grid_columnconfigure(0, weight=1)
    smart_card.grid_rowconfigure(0, weight=1)
    smart_card.grid_rowconfigure(1, weight=0)

    smart_btn = tk.Button(
        smart_card,
        text="스마트스토어 파일 불러오기",
        command=choose_smartstore,
        image=ss_logo,
        compound="top",
        bg=ss_bg,
        fg="#000000",
        relief="flat",
        bd=0,
        activebackground=ss_bg,
        highlightthickness=0,
        padx=12,
        pady=12,
    )
    smart_btn.grid(row=0, column=0, sticky="nsew")
    tk.Button(
        smart_card,
        text="로딩취소",
        command=clear_smartstore,
        bg=ss_bg,
        fg="#111111",
        relief="flat",
        bd=0,
        activebackground=ss_bg,
        highlightthickness=0,
    ).grid(row=1, column=0, pady=(0, 10))

    # Windows 고배율 환경에서 한쪽 버튼만 보이는 현상 방지를 위해 최소 폭 고정
    panel.update_idletasks()
    half_w = max((root.winfo_width() - 60) // 2, 340)
    panel.grid_columnconfigure(0, minsize=half_w)
    panel.grid_columnconfigure(1, minsize=half_w)

    # 상태 행은 파일명 가독성만 담당
    status_row = tk.Frame(container, bg="#f6f7f9")
    status_row.pack(fill="x", pady=10)
    left_status = tk.Frame(status_row, bg="#f6f7f9")
    left_status.pack(side="left", fill="x", expand=True)
    tk.Label(left_status, textvariable=cafe24_status, bg="#f6f7f9", anchor="w", width=42).pack(side="left", padx=(12, 6))

    right_status = tk.Frame(status_row, bg="#f6f7f9")
    right_status.pack(side="right", fill="x", expand=True)
    tk.Label(right_status, textvariable=smart_status, bg="#f6f7f9", anchor="e", width=42).pack(side="right", padx=(6, 12))

    output_row = tk.Frame(container, bg="#f6f7f9")
    output_row.pack(fill="x", pady=(16, 6))
    tk.Label(output_row, text="출력 폴더", bg="#f6f7f9").pack(side="left")
    output_entry = tk.Entry(output_row, textvariable=output_var, width=46, bg="#ffffff")
    output_entry.pack(side="left", padx=8)
    tk.Button(output_row, text="폴더 선택", command=choose_output, padx=10).pack(side="left", padx=10)
    tk.Button(output_row, text="저장 폴더 열기", command=open_output_folder, padx=10).pack(side="left")

    patch_row = tk.Frame(container, bg="#f6f7f9")
    patch_row.pack(fill="x", pady=(0, 10))
    tk.Button(
        patch_row,
        text="온라인 패치 업데이트",
        command=run_online_patch_update,
        padx=12,
        bg="#e8f0ff",
        fg="#1f3a78",
        relief="solid",
        bd=1,
    ).pack(side="right")

    run_btn = tk.Button(container, text="변환 시작", command=run, width=18, height=2)
    run_btn.pack(pady=12)

    tk.Label(container, textvariable=result_status, bg="#f6f7f9", font=("Apple SD Gothic Neo", 13, "bold")).pack()
    tk.Label(container, textvariable=result_detail, bg="#f6f7f9").pack()

    # 커스텀 규칙 관리 버튼
    def open_custom_rule_editor():
        CustomRuleEditorWindow(root)

    def open_gift_rule_editor():
        GiftRuleEditorWindow(root)

    custom_rule_btn = tk.Button(
        container,
        text="⚙ 커스텀 규칙 관리",
        command=open_custom_rule_editor,
        width=18,
        bg="#e8e8ea",
        fg="#333333",
        relief="solid",
        bd=1,
    )
    custom_rule_btn.pack(pady=6, ipady=4)

    gift_rule_btn = tk.Button(
        container,
        text="🎁 사은품 규칙 관리",
        command=open_gift_rule_editor,
        width=18,
        bg="#e8e8ea",
        fg="#333333",
        relief="solid",
        bd=1,
    )
    gift_rule_btn.pack(pady=2, ipady=4)

    enable_dnd(root, cafe24_var, smart_var, cafe24_status, smart_status)
    dnd_state = "ON" if is_dnd_enabled(root) else "OFF (현재 환경 미지원)"
    tk.Label(container, text=f"드래그&드랍: {dnd_state}", bg="#f6f7f9", fg="#666666").pack(pady=(6, 0))

    root.mainloop()


class GiftRuleEditorWindow:
    """사은품 전용 규칙 관리 GUI."""

    def __init__(self, parent):
        self.parent = parent
        self.store = GiftRuleStore(_resolve_config_path(os.path.join("configs", "gift_rules.json")) or get_gift_rules_path())
        self.rules: List[Dict[str, Any]] = list(self.store.get_rules())
        self.selected_idx: Optional[int] = None

        self.win = tk.Toplevel(parent)
        self.win.title("사은품 규칙 관리")
        self.win.geometry("1100x640")
        self.win.grab_set()

        top = tk.Frame(self.win)
        top.pack(fill="x", padx=10, pady=8)
        tk.Label(
            top,
            text="사은품은 커스텀 규칙에서 제외됩니다. 이 창에서만 관리하세요.",
            anchor="w",
            fg="#333333",
        ).pack(fill="x")

        body = tk.Frame(self.win)
        body.pack(fill="both", expand=True, padx=10, pady=6)

        left = tk.Frame(body)
        left.pack(side="left", fill="both", expand=False)
        tk.Label(left, text="저장된 사은품 규칙", font=("Apple SD Gothic Neo", 11, "bold")).pack(anchor="w")
        list_frame = tk.Frame(left)
        list_frame.pack(fill="both", expand=True, pady=4)
        ybar = tk.Scrollbar(list_frame, orient="vertical")
        ybar.pack(side="right", fill="y")
        xbar = tk.Scrollbar(list_frame, orient="horizontal")
        xbar.pack(side="bottom", fill="x")
        self.listbox = tk.Listbox(
            list_frame,
            width=38,
            height=26,
            yscrollcommand=ybar.set,
            xscrollcommand=xbar.set,
            exportselection=False,
        )
        self.listbox.pack(side="left", fill="both", expand=True)
        ybar.config(command=self.listbox.yview)
        xbar.config(command=self.listbox.xview)
        self.listbox.bind("<<ListboxSelect>>", self._on_select)

        right = tk.Frame(body)
        right.pack(side="right", fill="both", expand=True, padx=(14, 0))
        tk.Label(right, text="규칙 입력/수정", font=("Apple SD Gothic Neo", 11, "bold")).pack(anchor="w")

        row0 = tk.Frame(right)
        row0.pack(fill="x", pady=3)
        tk.Label(row0, text="규칙명", width=12, anchor="w").pack(side="left")
        self.rule_name_var = tk.StringVar(value="")
        tk.Entry(row0, textvariable=self.rule_name_var).pack(side="left", fill="x", expand=True)

        row1 = tk.Frame(right)
        row1.pack(fill="x", pady=3)
        tk.Label(row1, text="스토어", width=12, anchor="w").pack(side="left")
        self.store_var = tk.StringVar(value="공통")
        tk.Radiobutton(row1, text="공통", variable=self.store_var, value="공통").pack(side="left")
        tk.Radiobutton(row1, text="자사몰", variable=self.store_var, value="자사몰").pack(side="left")
        tk.Radiobutton(row1, text="스마트스토어", variable=self.store_var, value="스마트스토어").pack(side="left")

        row2 = tk.Frame(right)
        row2.pack(fill="x", pady=3)
        tk.Label(row2, text="매칭대상", width=12, anchor="w").pack(side="left")
        self.match_field_var = tk.StringVar(value="product")
        tk.Radiobutton(row2, text="상품명", variable=self.match_field_var, value="product").pack(side="left")
        tk.Radiobutton(row2, text="옵션문자열", variable=self.match_field_var, value="option").pack(side="left")

        row3 = tk.Frame(right)
        row3.pack(fill="x", pady=3)
        tk.Label(row3, text="포함문구", width=12, anchor="w").pack(side="left")
        self.contains_var = tk.StringVar(value="")
        tk.Entry(row3, textvariable=self.contains_var).pack(side="left", fill="x", expand=True)

        sep = ttk.Separator(right, orient="horizontal")
        sep.pack(fill="x", pady=8)

        tk.Label(right, text="출력 상품", font=("Apple SD Gothic Neo", 10, "bold")).pack(anchor="w")

        row4 = tk.Frame(right)
        row4.pack(fill="x", pady=3)
        tk.Label(row4, text="상품명", width=12, anchor="w").pack(side="left")
        self.out_product_var = tk.StringVar(value="")
        tk.Entry(row4, textvariable=self.out_product_var).pack(side="left", fill="x", expand=True)

        row5 = tk.Frame(right)
        row5.pack(fill="x", pady=3)
        tk.Label(row5, text="옵션(색상)", width=12, anchor="w").pack(side="left")
        self.out_color_var = tk.StringVar(value="")
        tk.Entry(row5, textvariable=self.out_color_var).pack(side="left", fill="x", expand=True)

        row6 = tk.Frame(right)
        row6.pack(fill="x", pady=3)
        tk.Label(row6, text="수량", width=12, anchor="w").pack(side="left")
        self.out_qty_var = tk.StringVar(value="1")
        tk.Entry(row6, textvariable=self.out_qty_var, width=10).pack(side="left")

        row7 = tk.Frame(right)
        row7.pack(fill="x", pady=3)
        tk.Label(row7, text="사이즈 모드", width=12, anchor="w").pack(side="left")
        self.size_mode_var = tk.StringVar(value="selected")
        tk.Radiobutton(row7, text="선택사이즈 사용", variable=self.size_mode_var, value="selected",
                       command=self._toggle_size_mode).pack(side="left")
        tk.Radiobutton(row7, text="고정값 사용", variable=self.size_mode_var, value="fixed",
                       command=self._toggle_size_mode).pack(side="left")

        row8 = tk.Frame(right)
        row8.pack(fill="x", pady=3)
        tk.Label(row8, text="고정 사이즈", width=12, anchor="w").pack(side="left")
        self.size_value_var = tk.StringVar(value="")
        self.entry_size_value = tk.Entry(row8, textvariable=self.size_value_var)
        self.entry_size_value.pack(side="left", fill="x", expand=True)

        row9 = tk.Frame(right)
        row9.pack(fill="x", pady=3)
        tk.Label(row9, text="사이즈 접미", width=12, anchor="w").pack(side="left")
        self.size_suffix_var = tk.StringVar(value="(증정)")
        self.entry_size_suffix = tk.Entry(row9, textvariable=self.size_suffix_var)
        self.entry_size_suffix.pack(side="left", fill="x", expand=True)

        btn = tk.Frame(right)
        btn.pack(fill="x", pady=(12, 2))
        tk.Button(btn, text="새로 입력", command=self._clear_form).pack(side="left")
        tk.Button(btn, text="삭제", command=self._delete_selected, fg="red").pack(side="left", padx=5)
        tk.Button(btn, text="기본값 복원", command=self._reset_default).pack(side="left", padx=5)
        tk.Button(btn, text="닫기", command=self.win.destroy).pack(side="right")
        tk.Button(btn, text="저장", command=self._save_rule, bg="#4a90d9", fg="white").pack(side="right", padx=6)

        self._refresh_list()
        self._toggle_size_mode()

    def _display_rule(self, rule: Dict[str, Any]) -> str:
        store = normalize_text(rule.get("store")) or "공통"
        field = "상품명" if normalize_text(rule.get("match_field")) == "product" else "옵션"
        contains = normalize_text(rule.get("contains"))
        out = rule.get("output", {})
        p = normalize_text(out.get("product")) if isinstance(out, dict) else ""
        c = normalize_text(out.get("color")) if isinstance(out, dict) else ""
        return f"[{store}] {field}:{contains} -> {p}/{c}"

    def _refresh_list(self) -> None:
        self.listbox.delete(0, tk.END)
        for rule in self.rules:
            self.listbox.insert(tk.END, self._display_rule(rule))

    def _on_select(self, _event=None) -> None:
        sel = self.listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < 0 or idx >= len(self.rules):
            return
        self.selected_idx = idx
        self._load_rule(self.rules[idx])

    def _load_rule(self, rule: Dict[str, Any]) -> None:
        out = rule.get("output", {})
        self.rule_name_var.set(normalize_text(rule.get("name")))
        self.store_var.set(normalize_text(rule.get("store")) or "공통")
        self.match_field_var.set(normalize_text(rule.get("match_field")) or "product")
        self.contains_var.set(normalize_text(rule.get("contains")))
        self.out_product_var.set(normalize_text(out.get("product")) if isinstance(out, dict) else "")
        self.out_color_var.set(normalize_text(out.get("color")) if isinstance(out, dict) else "")
        self.out_qty_var.set(normalize_text(out.get("qty")) if isinstance(out, dict) else "1")
        self.size_mode_var.set(normalize_text(out.get("size_mode")) if isinstance(out, dict) else "selected")
        self.size_value_var.set(normalize_text(out.get("size_value")) if isinstance(out, dict) else "")
        self.size_suffix_var.set(normalize_text(out.get("size_suffix")) if isinstance(out, dict) else "(증정)")
        self._toggle_size_mode()

    def _toggle_size_mode(self) -> None:
        mode = self.size_mode_var.get()
        if mode == "fixed":
            self.entry_size_value.configure(state="normal")
            self.entry_size_suffix.configure(state="disabled")
        else:
            self.entry_size_value.configure(state="disabled")
            self.entry_size_suffix.configure(state="normal")

    def _clear_form(self) -> None:
        self.selected_idx = None
        self.rule_name_var.set("")
        self.store_var.set("공통")
        self.match_field_var.set("product")
        self.contains_var.set("")
        self.out_product_var.set("")
        self.out_color_var.set("")
        self.out_qty_var.set("1")
        self.size_mode_var.set("selected")
        self.size_value_var.set("")
        self.size_suffix_var.set("(증정)")
        self._toggle_size_mode()
        self.listbox.selection_clear(0, tk.END)

    def _build_rule_from_form(self) -> Optional[Dict[str, Any]]:
        name = normalize_text(self.rule_name_var.get())
        store = normalize_text(self.store_var.get()) or "공통"
        match_field = normalize_text(self.match_field_var.get()) or "product"
        contains = normalize_text(self.contains_var.get())
        out_product = normalize_text(self.out_product_var.get())
        out_color = normalize_text(self.out_color_var.get())
        out_qty = normalize_text(self.out_qty_var.get()) or "1"
        size_mode = normalize_text(self.size_mode_var.get()) or "selected"
        size_value = normalize_text(self.size_value_var.get())
        size_suffix = normalize_text(self.size_suffix_var.get())

        if not name or not contains or not out_product:
            messagebox.showwarning("입력 오류", "규칙명/포함문구/출력상품명은 필수입니다.", parent=self.win)
            return None
        if size_mode == "fixed" and not size_value:
            messagebox.showwarning("입력 오류", "고정 사이즈 모드에서는 고정 사이즈를 입력하세요.", parent=self.win)
            return None

        return {
            "name": name,
            "store": store,
            "match_field": match_field,
            "contains": contains,
            "output": {
                "product": out_product,
                "color": out_color,
                "size_mode": size_mode,
                "size_value": size_value if size_mode == "fixed" else "",
                "size_suffix": size_suffix if size_mode != "fixed" else "",
                "qty": out_qty,
            },
        }

    def _save_rule(self) -> None:
        rule = self._build_rule_from_form()
        if rule is None:
            return
        if self.selected_idx is None:
            self.rules.append(rule)
        else:
            self.rules[self.selected_idx] = rule
        payload = {"rules": self.rules}
        try:
            self.store.set_payload(payload)
        except Exception as e:
            messagebox.showerror("저장 실패", str(e), parent=self.win)
            return
        self._refresh_list()
        messagebox.showinfo("저장 완료", "사은품 규칙이 저장되었습니다.", parent=self.win)

    def _delete_selected(self) -> None:
        sel = self.listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < 0 or idx >= len(self.rules):
            return
        if not messagebox.askyesno("삭제 확인", "선택한 사은품 규칙을 삭제할까요?", parent=self.win):
            return
        del self.rules[idx]
        try:
            self.store.set_payload({"rules": self.rules})
        except Exception as e:
            messagebox.showerror("삭제 실패", str(e), parent=self.win)
            return
        self._refresh_list()
        self._clear_form()

    def _reset_default(self) -> None:
        if not messagebox.askyesno("기본값 복원", "사은품 규칙을 기본값으로 되돌릴까요?", parent=self.win):
            return
        self.store.reset_defaults()
        self.rules = list(self.store.get_rules())
        self._refresh_list()
        self._clear_form()



class CustomRuleEditorWindow:
    """configs/custom_rules.json을 편집하는 별도 GUI 윈도우."""

    def __init__(self, parent):
        self.parent = parent
        self.rule_store = CustomRuleStore(
            _resolve_config_path(os.path.join("configs", "custom_rules.json")) or get_custom_rules_path()
        )
        self.option_rows: List[Tuple[tk.Entry, tk.Entry, tk.Entry, tk.Entry]] = []
        self.list_keys: List[str] = []

        self.win = tk.Toplevel(parent)
        self.win.title("커스텀 규칙 관리")
        self.win.geometry("1100x620")
        self.win.grab_set()

        # ===== 좌측: 기존 규칙 목록 =====
        left_frame = tk.Frame(self.win)
        left_frame.pack(side="left", fill="both", expand=False, padx=10, pady=10)
        tk.Label(left_frame, text="저장된 규칙 목록", font=("Apple SD Gothic Neo", 11, "bold")).pack(anchor="w")

        list_frame = tk.Frame(left_frame)
        list_frame.pack(fill="both", expand=True, pady=5)
        scrollbar_y = tk.Scrollbar(list_frame, orient="vertical")
        scrollbar_y.pack(side="right", fill="y")
        scrollbar_x = tk.Scrollbar(list_frame, orient="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")
        self.rule_listbox = tk.Listbox(
            list_frame,
            width=30,
            height=20,
            yscrollcommand=scrollbar_y.set,
            xscrollcommand=scrollbar_x.set,
            exportselection=False,
        )
        self.rule_listbox.pack(side="left", fill="both", expand=True)
        scrollbar_y.config(command=self.rule_listbox.yview)
        scrollbar_x.config(command=self.rule_listbox.xview)
        self.rule_listbox.bind("<<ListboxSelect>>", self._on_rule_select)

        list_btn_frame = tk.Frame(left_frame)
        list_btn_frame.pack(fill="x", pady=5)
        tk.Button(list_btn_frame, text="삭제", command=self._delete_selected, fg="red").pack(side="right")

        # ===== 우측: 입력 폼 =====
        right_frame = tk.Frame(self.win)
        right_frame.pack(side="right", fill="both", expand=True, padx=10, pady=10)

        tk.Label(right_frame, text="새 규칙 추가", font=("Apple SD Gothic Neo", 11, "bold")).pack(anchor="w")

        # 스토어 타입
        store_frame = tk.Frame(right_frame)
        store_frame.pack(fill="x", pady=5)
        tk.Label(store_frame, text="스토어 타입:").pack(side="left")
        self.store_type_var = tk.StringVar(value="자사몰")
        tk.Radiobutton(store_frame, text="자사몰", variable=self.store_type_var, value="자사몰",
                       command=self._update_labels).pack(side="left", padx=5)
        tk.Radiobutton(store_frame, text="스마트스토어", variable=self.store_type_var, value="스마트스토어",
                       command=self._update_labels).pack(side="left", padx=5)

        # 주문상품명 / 상품명
        name_frame = tk.Frame(right_frame)
        name_frame.pack(fill="x", pady=3)
        self.lbl_product = tk.Label(name_frame, text="주문상품명:", width=16, anchor="w")
        self.lbl_product.pack(side="left")
        self.entry_product = tk.Text(
            name_frame,
            width=40,
            height=2,
            wrap="word",
            bd=2,
            relief="solid",
            highlightbackground="#666666",
            highlightcolor="#666666",
            highlightthickness=1,
        )
        self.entry_product.pack(side="left", fill="x", expand=True)

        # 주문상품명(옵션포함) / 옵션정보
        opt_name_frame = tk.Frame(right_frame)
        opt_name_frame.pack(fill="x", pady=3)
        self.lbl_option_name = tk.Label(opt_name_frame, text="주문명(옵션포함):", width=16, anchor="w")
        self.lbl_option_name.pack(side="left")
        self.entry_option_name = tk.Text(
            opt_name_frame,
            width=40,
            height=3,
            wrap="word",
            bd=2,
            relief="solid",
            highlightbackground="#666666",
            highlightcolor="#666666",
            highlightthickness=1,
        )
        self.entry_option_name.pack(side="left", fill="x", expand=True)

        self.entry_product.bind("<KeyRelease>", lambda _e: self._highlight_current_rule())
        self.entry_option_name.bind("<KeyRelease>", lambda _e: self._highlight_current_rule())
        self.entry_product.bind("<ButtonRelease-1>", lambda _e: self.win.after_idle(self._highlight_current_rule))
        self.entry_option_name.bind("<ButtonRelease-1>", lambda _e: self.win.after_idle(self._highlight_current_rule))
        self.entry_product.bind("<Double-Button-1>", lambda _e: self.win.after_idle(self._highlight_current_rule))
        self.entry_option_name.bind("<Double-Button-1>", lambda _e: self.win.after_idle(self._highlight_current_rule))
        self.entry_product.bind("<FocusIn>", lambda _e: self.win.after_idle(self._highlight_current_rule))
        self.entry_option_name.bind("<FocusIn>", lambda _e: self.win.after_idle(self._highlight_current_rule))

        # 옵션 행 테이블
        tk.Label(right_frame, text="출력 옵션 행:", font=("Apple SD Gothic Neo", 10)).pack(anchor="w", pady=(8, 0))
        tip_row = tk.Frame(right_frame)
        tip_row.pack(fill="x")
        tk.Label(
            tip_row,
            text="안내: 동일 옵션은 여러 줄로 그대로 입력하세요. (최종 변환 시 자동 합산)",
            fg="#3a3a3a",
            anchor="w",
        ).pack(side="left")
        tip_icon = tk.Label(
            tip_row,
            text=" (?) ",
            fg="#1e5aa8",
            cursor="question_arrow",
        )
        tip_icon.pack(side="left", padx=(4, 0))
        HoverTooltip(
            tip_icon,
            "입력 예시: 블랙 L, 내추럴스킨 L, 블랙 L\n"
            "처럼 중복 입력해도 저장 가능합니다.\n"
            "엑셀 변환 시 주문번호+출고명 기준으로\n"
            "수량이 자동 합산됩니다.",
        )

        # grid 기반 테이블 컨테이너
        self.table_frame = tk.Frame(right_frame)
        self.table_frame.pack(fill="both", expand=True, pady=3)

        # 컬럼 폭 설정 (1100px 창 - 좌측패널 고려, 우측 약 650px)
        col_widths = [160, 130, 80, 60, 30]  # 상품명, 옵션(색상), 사이즈, 수량, 삭제버튼
        for i, w in enumerate(col_widths):
            self.table_frame.columnconfigure(i, minsize=w)

        # 헤더 행
        headers = ["상품명", "옵션(색상)", "사이즈", "수량", ""]
        for col, text in enumerate(headers):
            tk.Label(self.table_frame, text=text, anchor="w",
                     font=("Apple SD Gothic Neo", 9, "bold")).grid(
                row=0, column=col, sticky="w", padx=4, pady=(0, 4))

        self._grid_row_counter = 1  # 헤더가 row 0

        self.rows_container = self.table_frame  # _add_option_row에서 사용

        btn_row = tk.Frame(right_frame)
        btn_row.pack(fill="x", pady=5)
        tk.Button(btn_row, text="+ 행 추가", command=self._add_option_row).pack(side="left")
        tk.Button(btn_row, text="- 행 삭제", command=self._remove_last_option_row, fg="red").pack(side="left", padx=5)
        tk.Button(btn_row, text="저장", command=self._save_rule,
                  bg="#4a90d9", fg="white", highlightbackground="#4a90d9",
                  activebackground="#3a7cc0").pack(side="right")
        tk.Button(btn_row, text="초기화", command=self._clear_form).pack(side="right", padx=5)

        # 초기 행 추가 및 목록 로드
        self._add_option_row()
        self._refresh_list()

    def _update_labels(self) -> None:
        if self.store_type_var.get() == "자사몰":
            self.lbl_product.config(text="주문상품명:")
            self.lbl_option_name.config(text="주문명(옵션포함):")
        else:
            self.lbl_product.config(text="상품명:")
            self.lbl_option_name.config(text="옵션정보:")
        self._highlight_current_rule()

    def _get_multiline_text(self, widget: tk.Text) -> str:
        return widget.get("1.0", "end-1c").strip()

    def _set_multiline_text(self, widget: tk.Text, value: str) -> None:
        widget.delete("1.0", "end")
        widget.insert("1.0", value)

    def _current_rule_key(self) -> str:
        store = self.store_type_var.get()
        product = normalize_text(self._get_multiline_text(self.entry_product))
        option_name = normalize_text(self._get_multiline_text(self.entry_option_name))
        if not product:
            return ""
        return f"스토어타입={store}|{product}|{option_name}"

    def _add_option_row(self) -> None:
        r = self._grid_row_counter
        e_prod = tk.Entry(self.rows_container, width=20)
        e_color = tk.Entry(self.rows_container, width=16)
        e_size = tk.Entry(self.rows_container, width=10)
        e_qty = tk.Entry(self.rows_container, width=6)
        e_prod.grid(row=r, column=0, sticky="w", padx=4, pady=1)
        e_color.grid(row=r, column=1, sticky="w", padx=4, pady=1)
        e_size.grid(row=r, column=2, sticky="w", padx=4, pady=1)
        e_qty.grid(row=r, column=3, sticky="w", padx=4, pady=1)
        e_qty.insert(0, "1")
        del_btn = tk.Button(self.rows_container, text="-",
                            command=lambda: self._remove_option_row_grid(r),
                            bg="#ffffff", fg="red", relief="solid", bd=1,
                            highlightbackground="red")
        del_btn.grid(row=r, column=4, padx=4, pady=1)
        self.option_rows.append((e_prod, e_color, e_size, e_qty))
        self._grid_row_counter += 1

    def _remove_option_row(self, row_frame) -> None:
        row_frame.destroy()
        self.option_rows = [r for r in self.option_rows if r[0].winfo_exists()]

    def _remove_option_row_grid(self, row_index: int) -> None:
        """Grid 레이아웃에서 특정 행을 제거한다."""
        # 해당 행의 모든 위젯을 파괴
        for widget in self.rows_container.grid_slaves(row=row_index):
            widget.destroy()
        # option_rows 리스트에서도 해당 엔트리 튜플 제거
        # grid_slaves로 위젯을 찾아서 제거하는 방식은 복잡하므로,
        # option_rows 리스트를 재구성하는 방식으로 처리
        # (e_prod, e_color, e_size, e_qty) 튜플 중 e_prod가 파괴되었는지 확인
        self.option_rows = [r for r in self.option_rows if r[0].winfo_exists()]
        # 행이 제거된 후, 아래 행들을 위로 당겨 정렬
        self._repack_grid_rows()

    def _remove_last_option_row(self) -> None:
        """마지막 옵션 행을 삭제한다."""
        self.option_rows = [r for r in self.option_rows if r[0].winfo_exists()]
        if not self.option_rows:
            return
        last = self.option_rows.pop()
        # grid에서 마지막 행의 위젯들 삭제
        grid_row = last[0].grid_info().get("row", None)
        if grid_row is not None:
            for widget in self.rows_container.grid_slaves(row=grid_row):
                widget.destroy()

    def _repack_grid_rows(self) -> None:
        """기존 옵션 행을 row 1부터 재배치한다."""
        self.option_rows = [r for r in self.option_rows if r[0].winfo_exists()]
        for idx, (ep, ec, es, eq) in enumerate(self.option_rows):
            new_row = idx + 1  # row 0 = 헤더
            ep.grid_configure(row=new_row)
            ec.grid_configure(row=new_row)
            es.grid_configure(row=new_row)
            eq.grid_configure(row=new_row)
            # 같은 행의 삭제 버튼도 이동
            for w in self.rows_container.grid_slaves(row=new_row + 1):
                pass  # 이미 재배치됨
        self._grid_row_counter = len(self.option_rows) + 1

    def _refresh_list(self) -> None:
        self.rule_listbox.delete(0, tk.END)
        self.list_keys = sorted(self.rule_store.data.keys())
        for key in self.list_keys:
            parts = key.split("|")
            store = parts[0].replace("스토어타입=", "")
            tag = "[SS]" if store == "스마트스토어" else "[CA]"
            rest = " / ".join(parts[1:])
            display = f"{tag} {rest}"
            self.rule_listbox.insert(tk.END, display)
        self._highlight_current_rule()

    def _on_rule_select(self, event) -> None:
        sel = self.rule_listbox.curselection()
        if not sel:
            return
        if sel[0] < len(self.list_keys):
            key = self.list_keys[sel[0]]
            self._load_rule_to_form(key)

    def _highlight_current_rule(self) -> None:
        key = self._current_rule_key()
        self.rule_listbox.selection_clear(0, tk.END)
        idx = -1
        if key and key in self.list_keys:
            idx = self.list_keys.index(key)
        else:
            product = normalize_text(self._get_multiline_text(self.entry_product))
            store = self.store_type_var.get()
            if product:
                prefix = f"스토어타입={store}|{product}|"
                for i, k in enumerate(self.list_keys):
                    if k.startswith(prefix):
                        idx = i
                        break
        if idx >= 0:
            self.rule_listbox.selection_set(idx)
            self.rule_listbox.activate(idx)
            self.rule_listbox.see(idx)

    def _load_rule_to_form(self, key: str) -> None:
        # 파싱: '스토어타입=자사몰|제품명|옵션명'
        parts = key.split("|")
        store_part = parts[0] if parts else ""
        product = parts[1] if len(parts) > 1 else ""
        option_name = parts[2] if len(parts) > 2 else ""

        if "자사몰" in store_part:
            self.store_type_var.set("자사몰")
        else:
            self.store_type_var.set("스마트스토어")
        self._update_labels()

        self._set_multiline_text(self.entry_product, product)
        self._set_multiline_text(self.entry_option_name, option_name)

        # 기존 옵션 행 제거 (헤더 row 0은 보존)
        self._clear_option_rows()

        # 기존 데이터 로드
        data = self.rule_store.get(key) or []
        for item in data:
            self._add_option_row()
            ep, ec, es, eq = self.option_rows[-1]
            ep.insert(0, item.get("product", ""))
            ec.insert(0, item.get("color", ""))
            es.insert(0, item.get("size", ""))
            eq.delete(0, tk.END)
            eq.insert(0, item.get("qty", "1"))

        if not data:
            self._add_option_row()
        self._highlight_current_rule()

    def _save_rule(self) -> None:
        store = self.store_type_var.get()
        product = normalize_text(self._get_multiline_text(self.entry_product))
        option_name = normalize_text(self._get_multiline_text(self.entry_option_name))

        if not product:
            messagebox.showwarning("입력 오류", "상품명을 입력해주세요.", parent=self.win)
            return

        items: List[Dict[str, str]] = []
        for ep, ec, es, eq in self.option_rows:
            if not ep.winfo_exists():
                continue
            p = normalize_text(ep.get())
            c = normalize_text(ec.get())
            s = normalize_text(es.get())
            q = normalize_text(eq.get()) or "1"
            if p:
                items.append({"product": p, "color": c, "size": s, "qty": q})
        if not items:
            messagebox.showwarning("입력 오류", "최소 1개의 옵션 행을 입력해주세요.", parent=self.win)
            return
        # 정책: 사은품/증정 항목은 custom_rules가 아닌 gift_rules에서만 관리한다.
        for item in items:
            if is_gift_like_rule_item(item):
                messagebox.showwarning(
                    "입력 오류",
                    "사은품/증정 항목은 커스텀 규칙에 저장할 수 없습니다.\n"
                    "메인 화면의 [사은품 규칙 관리]에서 설정해주세요.",
                    parent=self.win,
                )
                return

        key = f"스토어타입={store}|{product}|{option_name}"
        self.rule_store.set(key, items)
        messagebox.showinfo("저장 완료", f"규칙이 저장되었습니다.\n키: {key}", parent=self.win)
        self._refresh_list()
        self._clear_form()

    def _clear_option_rows(self) -> None:
        """헤더(row 0)를 제외한 모든 옵션 행 위젯을 삭제한다."""
        for widget in self.rows_container.grid_slaves():
            info = widget.grid_info()
            if int(info.get("row", 0)) > 0:
                widget.destroy()
        self.option_rows.clear()
        self._grid_row_counter = 1

    def _clear_form(self) -> None:
        self._set_multiline_text(self.entry_product, "")
        self._set_multiline_text(self.entry_option_name, "")
        self._clear_option_rows()
        self._add_option_row()
        self._highlight_current_rule()

    def _delete_selected(self) -> None:
        sel = self.rule_listbox.curselection()
        if not sel:
            return
        if sel[0] >= len(self.list_keys):
            return
        key = self.list_keys[sel[0]]
        if messagebox.askyesno("삭제 확인", f"이 규칙을 삭제하시겠습니까?\n{key}", parent=self.win):
            del self.rule_store.data[key]
            self.rule_store.save()
            self._refresh_list()
            self._clear_form()


class HoverTooltip:
    """간단한 마우스 오버 툴팁."""

    def __init__(self, widget, text: str):
        self.widget = widget
        self.text = text
        self.tip = None
        self.widget.bind("<Enter>", self._show)
        self.widget.bind("<Leave>", self._hide)

    def _show(self, _event=None):
        if self.tip is not None:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tip = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            bg="#ffffe8",
            relief="solid",
            bd=1,
            padx=8,
            pady=6,
        )
        label.pack()

    def _hide(self, _event=None):
        if self.tip is None:
            return
        self.tip.destroy()
        self.tip = None


def create_gui_root():
    # macOS + Tk 9 계열에서 tkinterdnd2(tkdnd) 사용 시 앱이 크래시 나는 경우가 있어
    # 해당 조합은 강제로 일반 Tk 루트로 동작시킨다.
    if should_disable_tkdnd():
        return tk.Tk()
    try:
        from tkinterdnd2 import TkinterDnD
        root = TkinterDnD.Tk()
        root._is_dnd_root = True
        return root
    except Exception:
        return tk.Tk()


def force_single_root(root):
    try:
        default_root = tk._default_root  # type: ignore[attr-defined]
        if default_root is not None and default_root is not root:
            try:
                default_root.destroy()
            except Exception:
                pass
        try:
            tk._default_root = root  # type: ignore[attr-defined]
        except Exception:
            pass
    except Exception:
        return


def cleanup_extra_windows(root):
    try:
        windows = root.tk.call("wm", "stackorder", ".")
        if isinstance(windows, str):
            windows = windows.split()
        for w in windows:
            if w == root._w:
                continue
            try:
                widget = root.nametowidget(w)
                if widget.winfo_class() in ("Tk", "Toplevel"):
                    title = ""
                    try:
                        title = widget.title()
                    except Exception:
                        title = ""
                    if title.strip().lower() in ("tk", ""):
                        widget.destroy()
            except Exception:
                continue
    except Exception:
        pass


def schedule_cleanup_extra_windows(root):
    # 일부 환경에서 TkinterDnD가 빈 Tk 창을 생성하므로 반복 정리
    def _clean_loop(count=0):
        cleanup_extra_windows(root)
        if count < 8:
            root.after(100, lambda: _clean_loop(count + 1))

    root.after(50, _clean_loop)


def find_gui_python(subprocess):
    override = os.environ.get("EXCEL_AUTO_PY")
    if override:
        candidates = [override]
    else:
        candidates = [
            str(get_app_dir() / "venv312" / "bin" / "python"),
            str(get_app_dir() / "venv" / "bin" / "python"),
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3",
            "/Library/Frameworks/Python.framework/Versions/3.11/bin/python3",
            "/Library/Frameworks/Python.framework/Versions/3.10/bin/python3",
        ]
    test_code = (
        "import tkinter as tk; import pandas as pd; "
        "r=tk.Tk(); r.withdraw(); r.update_idletasks(); r.destroy(); "
        "print('OK')"
    )
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            result = subprocess.run([path, "-c", test_code], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and "OK" in result.stdout:
                return path
        except Exception:
            continue
    return None


def gui_self_test(subprocess, exe_path: str) -> bool:
    test_code = (
        "import tkinter as tk; import pandas as pd; "
        "r=tk.Tk(); r.withdraw(); r.update_idletasks(); r.destroy(); "
        "print('OK')"
    )
    try:
        result = subprocess.run([exe_path, "-c", test_code], capture_output=True, text=True, timeout=5)
        return result.returncode == 0 and "OK" in result.stdout
    except Exception:
        return False


def load_local_icon(master, path: Path, max_width: int, max_height: int):
    if tk is None or not path.exists():
        return None
    try:
        img = tk.PhotoImage(file=str(path), master=master)
        w, h = img.width(), img.height()
        if w <= 0 or h <= 0:
            return img
        ratio_w = max(1, w // max_width) if w > max_width else 1
        ratio_h = max(1, h // max_height) if h > max_height else 1
        ratio = max(ratio_w, ratio_h)
        if ratio > 1:
            img = img.subsample(ratio, ratio)
        return img
    except Exception:
        return None


def sample_image_bg_color(master, img):
    try:
        color = img.get(2, 2)
        if isinstance(color, tuple) and len(color) == 3:
            r, g, b = color
            return f"#{int(r):02x}{int(g):02x}{int(b):02x}"
        if isinstance(color, str):
            r, g, b = master.winfo_rgb(color)
            return f"#{r//256:02x}{g//256:02x}{b//256:02x}"
    except Exception:
        return None
    return None


def enable_dnd(root, cafe24_var, smart_var, cafe24_status=None, smart_status=None):
    if should_disable_tkdnd():
        return
    try:
        from tkinterdnd2 import DND_FILES, TkinterDnD
    except Exception:
        return

    if not isinstance(root, TkinterDnD.Tk):
        return

    def handle_drop(event):
        files = parse_dnd_files(event.data)
        for file_path in files:
            if not file_path.lower().endswith(".xlsx"):
                continue
            store = detect_store_type(Path(file_path))
            if store == "cafe24":
                cafe24_var.set(file_path)
                if cafe24_status is not None:
                    cafe24_status.set(f"파일 로드됨: {compress_filename_for_status(file_path)}")
            elif store == "smartstore":
                smart_var.set(file_path)
                if smart_status is not None:
                    smart_status.set(f"파일 로드됨: {compress_filename_for_status(file_path)}")
            else:
                if not cafe24_var.get():
                    cafe24_var.set(file_path)
                    if cafe24_status is not None:
                        cafe24_status.set(f"파일 로드됨: {compress_filename_for_status(file_path)}")
                elif not smart_var.get():
                    smart_var.set(file_path)
                    if smart_status is not None:
                        smart_status.set(f"파일 로드됨: {compress_filename_for_status(file_path)}")
        return

    def register_all(widget):
        try:
            widget.drop_target_register(DND_FILES)
            widget.dnd_bind("<<Drop>>", handle_drop)
        except Exception:
            pass
        for child in widget.winfo_children():
            register_all(child)

    register_all(root)


def is_dnd_enabled(root) -> bool:
    if should_disable_tkdnd():
        return False
    try:
        from tkinterdnd2 import TkinterDnD
    except Exception:
        return False
    return isinstance(root, TkinterDnD.Tk)


def should_disable_tkdnd() -> bool:
    if tk is None:
        return True
    # 현재 macOS 환경에서 tkinterdnd2(tkdnd) 로딩 시 SIGTRAP 크래시가 재현됨.
    # 안정성 우선으로 darwin에서는 기본 비활성화한다.
    if sys.platform == "darwin":
        return True
    return False


def parse_dnd_files(data: str) -> List[str]:
    paths = []
    buff = ""
    in_brace = False
    for ch in data:
        if ch == "{":
            in_brace = True
            buff = ""
        elif ch == "}":
            in_brace = False
            paths.append(buff)
            buff = ""
        elif ch == " " and not in_brace:
            if buff:
                paths.append(buff)
                buff = ""
        else:
            buff += ch
    if buff:
        paths.append(buff)
    return [p.strip() for p in paths if p.strip()]


def detect_store_type(path: Path) -> Optional[str]:
    try:
        header_idx = detect_header_row(path, "상품주문번호")
        df = pd.read_excel(path, sheet_name=0, header=header_idx, nrows=2)
        if "상품주문번호" in df.columns:
            return "smartstore"
    except Exception:
        pass
    try:
        header_idx = detect_header_row(path, "주문번호")
        df = pd.read_excel(path, sheet_name=0, header=header_idx, nrows=2)
        if "주문번호" in df.columns and ("주문상품명(옵션포함)" in df.columns or "옵션" in df.columns):
            return "cafe24"
    except Exception:
        pass
    return None


if __name__ == "__main__":
    main()
