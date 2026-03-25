import pandas as pd
import joblib


def load_model(path_model, experiment_name: str, run_id: str):
    name_file = path_model / f"{experiment_name}_{run_id}.joblib"
    bundle = joblib.load(name_file)
    model = bundle["model"]
    threshold = bundle["threshold"]

    return model, threshold


def get_probabilities(
    df_prep: pd.DataFrame,
    model,
    *,
    id_col: str = "id",
    has_id: bool | None = True,
    proba_col: str = "probability",
) -> pd.DataFrame:
    """
    Applies transformations to df_raw, computes predict_proba, and returns a DataFrame
    with the id column (if available) and probability outputs.
    """
    df = df_prep.copy()
    if has_id is None:
        has_id = id_col in df.columns

    ids = None
    if has_id:
        if id_col in df.columns:
            ids = df[id_col].copy()
            df = df.drop(columns=[id_col])
        else:
            has_id = None

    proba = model.predict_proba(df)[:, 1]
    out = pd.DataFrame({proba_col: proba}, index=df.index)
    if has_id:
        out.insert(0, id_col, ids.values)
    return out


def classify_from_proba(
    df_probs: pd.DataFrame,
    *,
    proba_col: str = "probability",
    threshold: float = 0.4036,
    class_col: str = "stays",
) -> dict:
    """
    Assigns a classification label from a probability column.

    For each row, a value of 1 (High probability) is assigned if `proba_col` exceeds `threshold`;
    otherwise, 0 (Low probability) is assigned.
    """
    df = df_probs.copy()
    stays = not (df[proba_col].iloc[0] > threshold)
    df[class_col] = stays
    return df.to_dict(orient="records")
