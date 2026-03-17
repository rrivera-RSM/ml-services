from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import yaml

@dataclass(frozen=True)
class ImputationConfig:
    survey_target_cols: tuple[str, ...]
    survey_imp_levels: tuple[str, ...]
    salary_target_cols: tuple[str, ...]
    salary_imp_levels: tuple[str, ...]

@dataclass(frozen=True)
class ScalingConfig:
    enabled: bool

@dataclass(frozen=True)
class FeatureConfig:
    id_col: str
    imputation: ImputationConfig
    scaling: ScalingConfig

@dataclass(frozen=True)
class AppConfig:
    ruleset_id: str
    effective_from: str
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
        raise ValueError("Config YAML inválida (no es un dict)")
    
    features = _as_dict(raw.get("features"), "features")
    imp = _as_dict(features.get("imputation"), "features.imputation")
    sca = _as_dict(features.get("scaling"), "features.scaling")

    cfg = AppConfig(
        ruleset_id=_req_str(raw, "ruleset_id", "root"),
        effective_from=_req_str(raw, "effective_from", "root"),

        features=FeatureConfig(
            id_col=_req_str(features, "id_col", "features"),
            imputation=ImputationConfig(
                survey_target_cols=tuple(map(str, _req_list(imp, "survey_target_cols", "features.imputation"))),
                survey_imp_levels=tuple(map(str, _req_list(imp, "survey_imp_levels", "features.imputation"))),
                salary_target_cols=tuple(map(str, _req_list(imp, "salary_target_cols", "features.imputation"))),
                salary_imp_levels=tuple(map(str, _req_list(imp, "salary_imp_levels", "features.imputation"))),
            ),
            scaling=ScalingConfig(
                enabled=_req_bool(sca, "enabled", "features.scaling")
            )
        )
    )

    return cfg