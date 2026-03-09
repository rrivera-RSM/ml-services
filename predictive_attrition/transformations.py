from __future__ import annotations

import numpy as np
import pandas as pd
from joblib import load
from pathlib import Path
from typing import Iterable
import yaml

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline

from .config import AppConfig

PathLike = str | Path


class EncuestaBlocks(BaseEstimator, TransformerMixin):
    def __init__(self, map_encuesta_bloque: dict):
        self.map_ = dict(map_encuesta_bloque)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        preguntas = [c for c in self.map_ if c in X.columns]
        if not preguntas:
            return X

        bloques = (
            X[preguntas].rename(columns=self.map_).apply(pd.to_numeric, errors="coerce")
        )
        bloques_mean = bloques.T.groupby(level=0).mean().T
        X = X.drop(columns=preguntas)
        return pd.concat([X, bloques_mean], axis=1)


class FeatureEngineer(BaseEstimator, TransformerMixin):
    def __init__(self, umbral_gastos: float, dieta_col: str = "DIETA"):
        self.umbral = umbral_gastos
        self.dieta_col = dieta_col

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        # Incremento Salarial
        X["Incremento Salarial"] = pd.to_numeric(
            X.get("Incremento Salarial", 0), errors="coerce"
        ).fillna(0)
        # Impacto positivo categórica
        imp = pd.to_numeric(X.get("Impacto positivo"), errors="coerce").fillna(0)
        X["Impacto positivo categorica"] = (imp > 0).astype(int)
        # Gastos (0/1/2)
        dieta = pd.to_numeric(X.get(self.dieta_col), errors="coerce").fillna(0)
        X["GASTOS"] = np.select(
            [dieta == 0, dieta <= self.umbral, dieta > self.umbral],
            [0, 1, 2],
            default=0,
        ).astype(int)
        # Categoria Profesional: Estructura, Gerente
        cat = X.get("Categoria Profesional")
        X["CATEGORIA_Estructura"] = (
            cat.fillna("").eq("Estructura").astype(int) if cat is not None else 0
        )
        X["CATEGORIA_Gerente"] = (
            cat.fillna("").isin(["Gerente", "Manager"]).astype(int)
            if cat is not None
            else 0
        )
        return X


class KNNImputeWithExpectedCols(BaseEstimator, TransformerMixin):
    """
    Applies OHE to cat_cols, aligns to expected_cols, and applies a pre-trained imputer.
    Returns the original DataFrame with the target_cols imputed (overwritten).
    """

    def __init__(
        self,
        imputer,
        expected_cols,
        target_cols,
        cat_cols=("SOCIEDAD", "OFICINA", "Categoria Profesional"),
    ):
        self.imputer = imputer
        self.expected_cols = list(expected_cols)
        self.target_cols = list(target_cols)
        self.cat_cols = tuple(cat_cols)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        # If the target contains no missing values, no action is taken
        if all(c in X.columns for c in self.target_cols):
            if X[self.target_cols].isnull().sum().sum() == 0:
                return X
        # ONE-HOT + align
        X_oh = pd.get_dummies(
            X, columns=[c for c in self.cat_cols if c in X.columns], drop_first=False
        )
        M = X_oh.reindex(columns=self.expected_cols, fill_value=0)
        M = M.apply(pd.to_numeric, errors="coerce")

        M_imp = pd.DataFrame(
            self.imputer.transform(M), columns=self.expected_cols, index=X.index
        )
        for c in self.target_cols:
            if c in M_imp.columns:
                X[c] = M_imp[c]
        return X


class MinMaxScaleInplace(BaseEstimator, TransformerMixin):
    """Applies a pre-trained scaler to the available cols_scale, ensuring alignment with the scaler's expected_cols."""

    def __init__(self, scaler, cols_scale, expected_cols=None):
        self.scaler = scaler
        self.cols_scale = list(cols_scale)
        self.expected_cols = list(expected_cols) if expected_cols is not None else None

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        expected = self.expected_cols or list(
            getattr(self.scaler, "feature_names_in_", [])
        )

        if not expected:
            expected = [c for c in self.cols_scale if c in X.columns]

        cols_present = [c for c in self.cols_scale if c in X.columns]
        if not cols_present:
            return X

        block = X.reindex(columns=expected)
        block.loc[:, cols_present] = block[cols_present].apply(
            pd.to_numeric, errors="coerce"
        )

        scaled = pd.DataFrame(
            self.scaler.transform(block), columns=expected, index=X.index
        )
        X.loc[:, cols_present] = scaled[cols_present]
        return X


class FinalAlign(BaseEstimator, TransformerMixin):
    """Selects, aligns, and returns ONLY the columns expected by the model, preserving order."""

    def __init__(
        self,
        model_expected_cols: Iterable[str],
        id_col: str = "id",
        include_id: bool = True,
        fill_value=0.0,
    ):
        self.model_expected_cols = list(model_expected_cols)
        self.id_col = id_col
        self.include_id = include_id
        self.fill_value = fill_value

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        cols = self.model_expected_cols
        if self.include_id:
            cols = [self.id_col] + cols
        return X.reindex(columns=cols, fill_value=self.fill_value)


def cargar_encuesta_bloques(ruta: PathLike) -> dict[str, str]:
    with open(ruta, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Formato no correcto")
    return {str(k): str(v) for k, v in data.items()}


def build_preprocessor(
    cfg: AppConfig,
    *,
    umbral_gastos: PathLike,
    encuesta_map_path: PathLike,
    imputer_enc_path: PathLike,
    imputer_sal_path: PathLike,
    scaler_path: PathLike | None = None,
    model_expected_cols: list[str] | None = None,
) -> Pipeline:
    """
    Builder: uses cfg.features to define:
    - cat_cols
    - imputation target columns
    - scaling configuration and cols_scale
    - id_col
    and takes the paths to artifacts as input parameters.
    """
    # Load artifacts
    umbral = load(umbral_gastos)
    enc = load(imputer_enc_path)
    sal = load(imputer_sal_path)
    map_encuesta_bloque = cargar_encuesta_bloques(encuesta_map_path)

    imputer_enc = enc["imputer"]
    expected_cols_enc = enc["expected_cols"]
    imputer_sal = sal["imputer"]
    expected_cols_sal = sal["expected_cols"]

    scaler = load(scaler_path) if scaler_path else None
    cols_fit_scale = (
        list(scaler.feature_names_in_)
        if scaler is not None and hasattr(scaler, "feature_names_in_")
        else None
    )

    cat_cols = cfg.features.cat_cols
    id_col = cfg.features.id_col
    enc_target_cols = list(cfg.features.imputation.enc_target_cols)
    sal_target_cols = list(cfg.features.imputation.sal_target_cols)

    steps: list[tuple[str, TransformerMixin]] = [
        ("encuestas", EncuestaBlocks(map_encuesta_bloque)),
        ("feat", FeatureEngineer(umbral_gastos=umbral)),
        (
            "imp_enc",
            KNNImputeWithExpectedCols(
                imputer=imputer_enc,
                expected_cols=expected_cols_enc,
                target_cols=enc_target_cols,
                cat_cols=cat_cols,
            ),
        ),
        (
            "imp_sal",
            KNNImputeWithExpectedCols(
                imputer=imputer_sal,
                expected_cols=expected_cols_sal,
                target_cols=sal_target_cols,
                cat_cols=cat_cols,
            ),
        ),
    ]
    if scaler is not None and cfg.features.scaling.enabled:
        steps.append(
            (
                "scale",
                MinMaxScaleInplace(
                    scaler=scaler,
                    cols_scale=list(cfg.features.scaling.cols_scale),
                    expected_cols=cols_fit_scale,
                ),
            )
        )

    if model_expected_cols is not None:
        steps.append(
            (
                "final_align",
                FinalAlign(
                    model_expected_cols=model_expected_cols,
                    id_col=id_col,
                    include_id=True,
                    fill_value=0.0,
                ),
            )
        )

    return Pipeline(steps)
