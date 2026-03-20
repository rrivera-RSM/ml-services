from __future__ import annotations
import numpy as np
import pandas as pd
from joblib import load
from pathlib import Path
from typing import Iterable
import yaml
import ast
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin

from predictive_attrition.config import AppConfig

PathLike = str | Path


class SurveysBlocks(BaseEstimator, TransformerMixin):
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
            X[preguntas]
            .rename(columns=self.map_)
            .apply(pd.to_numeric, errors="coerce")
        )
        bloques_mean = bloques.T.groupby(level=0).mean().T
        X = X.drop(columns=preguntas)
        return pd.concat([X, bloques_mean], axis=1)


class FeatureEngineer:
    def __init__(self, expenses_threshold):
        self.expenses_threshold = expenses_threshold
        self.salary_inc_col = "salary_increase"
        self.exp_col = "food"
        self.positive_imp_col = "bol_positive_impact"
        self.cat_col = "category"
        self.society_col = "society_name"

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        # salary increase
        X[self.salary_inc_col] = pd.to_numeric(
            X.get(self.salary_inc_col, 0), errors="coerce"
        ).fillna(0)
        X[self.salary_inc_col] = np.where(
            X[self.salary_inc_col] < 0, 0, X[self.salary_inc_col]
        )
        # expenses
        X[self.exp_col] = X[self.exp_col].clip(
            upper=self.expenses_threshold[self.exp_col]
        )
        X[self.exp_col] = np.log1p(X[self.exp_col])
        # bool cols
        X[self.positive_imp_col] = np.where(X[self.positive_imp_col] > 0, 1, 0)
        X[self.positive_imp_col] = X[self.positive_imp_col].astype("bool")
        X[f"{self.cat_col}_estructura"] = np.where(
            X[self.cat_col] == "Estructura", 1, 0
        )
        X[f"{self.cat_col}_manager"] = np.where(
            X[self.cat_col] == "Gerente", 1, 0
        )
        # society mapping
        X[self.society_col] = X[self.society_col].replace(
            {
                "Rsm Spain Servicios Administrativos, Sl": "servicios",
                "Rsm Spain Auditores, Slp": "auditoria",
                "Rsm Spain Asesores Legales Y Tributarios, Slp": "tax_legal",
                "Rsm Spain Consultores, Sl": "consulting",
                "Rsm Spain Corporate Finance, Sl": "corporate",
            }
        )
        return X


class ImputerMeanLevel(BaseEstimator, TransformerMixin):
    def __init__(self, imputer, target_cols, levels):
        self.imputer = imputer
        self.target_cols = target_cols
        self.levels = [
            ast.literal_eval(level) if isinstance(level, str) else level
            for level in levels
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        # If target contains no missing values, no action is taken
        if all(c in X.columns for c in self.target_cols):
            if X[self.target_cols].isnull().sum().sum() == 0:
                return X
        # impute
        for key in self.imputer.keys():
            if key in self.target_cols:
                for cols in self.levels:
                    med = self.imputer[key][tuple(cols)]
                    med = med.rename(columns={key: f"{key}_median"})
                    X = X.merge(med, on=cols, how="left")
                    X[key] = X[key].fillna(X[f"{key}_median"])
                    X = X.drop(columns=[f"{key}_median"])
                X[key] = X[key].fillna(self.imputer[key]["global"])
        return X


class MinMaxScale(BaseEstimator, TransformerMixin):
    """Applies a pre-trained scaler to the available cols_scale, ensuring alignment with the scaler's expected_cols."""

    def __init__(self, scaler, cols_scale, expected_cols=None):
        self.scaler = scaler
        self.cols_scale = list(cols_scale)
        self.expected_cols = (
            list(expected_cols) if expected_cols is not None else None
        )

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
        X.loc[:, cols_present] = scaled[cols_present].astype(float)
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


def load_survey_blocs(path: PathLike) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Format not valid")
    return {str(k): str(v) for k, v in data.items()}


def build_preprocessor(
    cfg: AppConfig,
    *,
    expenses_out_thrs: PathLike,
    b_survey_map_path: PathLike,
    b_survey_imputer_path: PathLike,
    salary_imputer_path: PathLike,
    scaler_path: PathLike | None = None,
    model_expected_cols: list[str] | None = None,
) -> Pipeline:
    """
    Builder: uses cfg.features
    and takes the paths to artifacts as input parameter.
    """

    # Load artifacts
    expenses_outliers_threshold = load(expenses_out_thrs)
    b_survey_imputer = load(b_survey_imputer_path)
    salary_imputer = load(salary_imputer_path)
    b_survey_map = load_survey_blocs(b_survey_map_path)

    scaler = load(scaler_path) if scaler_path else None
    cols_fit_scale = (
        list(scaler.feature_names_in_)
        if scaler is not None and hasattr(scaler, "feature_names_in_")
        else None
    )

    # column names
    id_col = cfg.features.id_col
    survey_target_cols = list(cfg.features.imputation.survey_target_cols)
    b_survey_levels = list(cfg.features.imputation.survey_imp_levels)
    salary_target_cols = list(cfg.features.imputation.salary_target_cols)
    salary_levels = list(cfg.features.imputation.salary_imp_levels)

    # Pipeline
    steps: list[tuple[str, TransformerMixin]] = [
        ("feature", FeatureEngineer(expenses_outliers_threshold)),
        ("surveys", SurveysBlocks(b_survey_map)),
        (
            "survey_imp",
            ImputerMeanLevel(
                imputer=b_survey_imputer,
                target_cols=survey_target_cols,
                levels=b_survey_levels,
            ),
        ),
        (
            "salary_imp",
            ImputerMeanLevel(
                imputer=salary_imputer,
                target_cols=salary_target_cols,
                levels=salary_levels,
            ),
        ),
    ]
    if scaler is not None and cfg.features.scaling.enabled:
        steps.append(
            (
                "scale",
                MinMaxScale(
                    scaler=scaler,
                    cols_scale=list(scaler.feature_names_in_),
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
