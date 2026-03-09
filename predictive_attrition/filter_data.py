from __future__ import annotations

from typing import Iterable, Tuple, Dict
import pandas as pd
import numpy as np
from pathlib import Path
from predictive_attrition.config import AppConfig


def load_idoposition(path_root: Path) -> set[str]:
    """
    Load ids
    """
    id_oposicion = pd.read_excel(path_root / "data/oposicion_modelo.xlsx")
    id_no_modelo = set(id_oposicion["id_no_modelo"].astype(str).tolist())
    return id_no_modelo


def add_is_first_year(
    df: pd.DataFrame,
    *,
    fiscal_year: int,
    joined_at_col: str = "joined_at",
    out_col: str = "is_first_year",
) -> pd.DataFrame:
    """
    Adds an is_first_year flag set to 1 if joined_at falls within the fiscal year (September–August),
    that is, in the interval [September 1 of fiscal_year, September 1 of fiscal_year + 1).
    """
    df = df.copy()
    if joined_at_col not in df.columns:
        df[out_col] = 0
        return df

    joined = pd.to_datetime(df[joined_at_col], errors="coerce")
    fy_start = pd.Timestamp(year=fiscal_year, month=9, day=1)
    fy_end = pd.Timestamp(year=fiscal_year + 1, month=9, day=1)

    df[out_col] = (joined.ge(fy_start) & joined.lt(fy_end)).astype(int)
    return df


def _resolve_today(cfg: AppConfig, today: str | pd.Timestamp | None) -> pd.Timestamp:
    """
    Priority order:
    1) `today` argument passed to the function
    2) `cfg.run.today` (if present in the YAML)
    3) `Timestamp.today()`
    """
    if today is not None:
        return pd.to_datetime(today).normalize()
    if cfg.run.today:
        return pd.to_datetime(cfg.run.today).normalize()
    return pd.Timestamp.today().normalize()


def build_filter_mask(
    df: pd.DataFrame,
    *,
    cfg: AppConfig,
    id_no_model: Iterable[str],
    today: str | pd.Timestamp | None = None,
) -> Dict[str, pd.Series]:
    """
    Defines filters as boolean masks.
    Dictionary keys represent the reasons to be stored in `motivo_exclusion`.
    """
    category_col = cfg.filters.categoria_col
    office_col = cfg.filters.oficina_col
    active_col = cfg.filters.activo_col
    id_col = cfg.features.id_col

    d = df.copy()
    if id_col not in d.columns:
        raise KeyError(f"Column '{id_col}' is missing in the dataset")
    d[id_col] = d[id_col].astype(str)

    today_ts = _resolve_today(cfg, today)
    id_no_model_set = set(map(str, id_no_model))

    masks: Dict[str, pd.Series] = {}

    # Exclude oposition model ids
    masks["opposition_model"] = d[id_col].isin(id_no_model_set)
    # Category not allowed
    if cfg.filters.allowed_categorias and category_col in d.columns:
        masks["not_allowed_category"] = ~d[category_col].isin(
            cfg.filters.allowed_categorias
        )
    # Office not allowed
    if cfg.filters.rejected_oficinas and office_col in d.columns:
        masks["not_allowed_office"] = d[office_col].isin(cfg.filters.rejected_oficinas)
    # Inactive
    if active_col in d.columns:
        left = pd.to_datetime(d[active_col], errors="coerce")
        masks["not_active"] = left.notna() & (left <= today_ts)

    return masks


def apply_filters(
    df: pd.DataFrame, *, masks: Dict[str, pd.Series], motive_col: str = "motive"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Applies the boolean masks:
    - df_ok: rows that pass all filters
    - df_excl: excluded rows, including a 'motive' column
    """
    out = df.copy()

    motive = pd.Series("", index=out.index, dtype="string")
    for reason, m in masks.items():
        m = m.reindex(out.index, fill_value=False)
        motive = np.where(
            m, np.where(motive == "", reason, (motive + "; " + reason)), motive
        )
    out[motive_col] = motive
    # Split into ok (valid rows for inference) vs excluded (non valid)
    excluded_mask = motive != ""

    df_excluded = out.loc[excluded_mask].copy()
    df_ok = out.loc[~excluded_mask].drop(columns=[motive_col]).copy()

    return df_ok, df_excluded


def filter_data(
    dataset: pd.DataFrame,
    *,
    cfg: AppConfig,
    id_no_model: Iterable[str],
    today: str | pd.Timestamp | None = None,
    add_first_year_flag: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filtering pipeline:
    - filter by fiscal year
    - optionally add is_first_year
    - apply filters
    """
    df = dataset.copy()

    # is_first_year
    if add_first_year_flag:
        df = add_is_first_year(df, fiscal_year=cfg.run.fiscal_year)
    # Apply masks
    masks = build_filter_mask(df, cfg=cfg, id_no_model=id_no_model, today=today)
    return apply_filters(df, masks=masks)


def apply_salary_simulation(
    df: pd.DataFrame,
    *,
    current_salary_col: str = "SALARIO NUEVO",
    new_salary_col: str = "SALARIO SIMULACION",
    increase_col: str = "Incremento Salarial",
) -> pd.DataFrame:
    """
    Recomputes Salary Increase when a salary change is detected.
    If no change is present, applies the no_change_model policy.

    The increase is computed as: (new - current) / current.
    - New Salary must be higher than current salary
    """
    out = df.copy()

    sal_act = pd.to_numeric(out.get(current_salary_col), errors="coerce")
    if new_salary_col not in out.columns:
        out[increase_col] = 0.0
        return out
    sal_new = pd.to_numeric(out[new_salary_col], errors="coerce")

    # Change applies (simulation) when a new salary exists and exceeds the current salary
    changed = (~sal_new.isna()) & (~sal_act.isna()) & (sal_new > sal_act)

    # Calculate the increase only for changed cases; keep the rest as NaN (0)
    denom = sal_act.replace(0, np.nan)
    inc = (sal_new - sal_act) / denom
    inc = inc.replace([np.inf, -np.inf], np.nan)

    out[increase_col] = inc.where(changed, 0.0).fillna(0.0)
    out.loc[changed, current_salary_col] = out.loc[changed, new_salary_col]

    return out
