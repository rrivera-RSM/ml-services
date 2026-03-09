from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import yaml

@dataclass(frozen=True)
class RunConfig: 
    fiscal_year: int
    today: Optional[str] = None

@dataclass(frozen=True)
class FilterConfig:
    categoria_col: str
    oficina_col: str
    activo_col: str
    fiscal_col: str

    allowed_categorias: frozenset[str]
    rejected_oficinas: frozenset[str]

@dataclass(frozen=True)
class ImputationConfig:
    enc_target_cols: tuple[str, ...]
    sal_target_cols: tuple[str, ...]

@dataclass(frozen=True)
class ScalingConfig:
    enabled: bool
    cols_scale: tuple[str, ...]

@dataclass(frozen=True)
class FeatureConfig:
    id_col: str
    cat_cols: tuple[str, ...]
    imputation: ImputationConfig
    scaling: ScalingConfig

@dataclass(frozen=True)
class AppConfig:
    ruleset_id: str
    effective_from: str
    run: RunConfig
    filters: FilterConfig
    features: FeatureConfig


# Helpers

def _as_dict(x: Any, ctx: str) -> dict[str, Any]:
    """Returns dict or {}, and validate type"""
    if x is None:
        return {}
    if not isinstance(x, dict):
        raise ValueError(f"'{ctx}' must be a YAML dict")
    return x

def _require(d: dict[str, Any], key: str,ctx: str) -> Any:
    """Demands key"""
    if key not in d:
        raise ValueError(f"Missing key '{ctx}.{key}'.")
    v = d[key]
    if v is None: 
        raise ValueError(f"'{ctx}.{key}' can't be null.")
    return v

def _req_str(d: dict[str, Any], key: str, ctx: str) -> str:
    return str(_require(d, key, ctx))
def _req_int(d: dict[str, Any], key: str, ctx: str) -> int:
    return int(_require(d, key, ctx))
def _req_bool(d: dict[str, Any], key: str, ctx: str) -> bool:
    v = _require(d, key, ctx)
    if isinstance(v, bool):
        return v
    raise ValueError(f"'{ctx}.{key}' must be boolean (true/false)")
def _req_list(d: dict[str, Any], key: str, ctx: str) -> list[Any]:
    v = _require(d, key, ctx)
    if isinstance(v, list):
        return v
    raise ValueError(f"'{ctx}.{key}' must be a list")


# Loader

def load_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config YAML inválida (no es un dict).")
    
    run = _as_dict(raw.get("run"), "run")
    filters = _as_dict(raw.get("filters"), "filters")
    features = _as_dict(raw.get("features"), "features")
    imp = _as_dict(features.get("imputation"), "features.imputation")
    sca = _as_dict(features.get("scaling"), "features.scaling")

    cfg = AppConfig(
        ruleset_id=_req_str(raw, "ruleset_id", "root"),
        effective_from=_req_str(raw, "effective_from", "root"),

        run=RunConfig(
            fiscal_year= _req_int(run, "fiscal_year", "run"),
            today=run.get("today", None),
        ),

        filters=FilterConfig(
            categoria_col=_req_str(filters, "categoria_col", "filters"),
            oficina_col=_req_str(filters, "oficina_col", "filters"),
            activo_col=_req_str(filters, "activo_col", "filters"),
            fiscal_col=_req_str(filters, "fiscal_col", "filters"),
            allowed_categorias=frozenset(map(str, _req_list(filters, "allowed_categorias", "filters"))),
            rejected_oficinas=frozenset(map(str, _req_list(filters, "rejected_oficinas", "filters"))),
        ),

        features=FeatureConfig(
            id_col=_req_str(features, "id_col", "features"),
            cat_cols=tuple(map(str, _req_list(features, "cat_cols", "features"))),
            imputation=ImputationConfig(
                enc_target_cols=tuple(map(str, _req_list(imp, "enc_target_cols", "features.imputation"))),
                sal_target_cols=tuple(map(str, _req_list(imp, "sal_target_cols", "features.imputation"))),
            ),
            scaling=ScalingConfig(
                enabled=_req_bool(sca, "enabled", "features.scaling"),
                cols_scale=tuple(map(str, _req_list(sca, "cols_scale", "features.scaling"))),
            ),
        ),
    )
    return cfg