import pandas as pd


def get_probabilities(
    df_prep: pd.DataFrame,
    model,
    *,
    id_col: str = "id",
    has_id: bool | None = True,
    proba_col: str = "probability",
) -> pd.DataFrame:
    """
    Applies transformations to df_raw, computes predict_proba, and returns a DataFrame with the id column (if available) and probability outputs.
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


def get_classification(
    df_prep: pd.DataFrame,
    model,
    *,
    threshold: float = 0.5,
    id_col: str = "id",
    has_id: bool | None = True,
    proba_col: str = "probability",
    class_col: str = "class",
) -> pd.DataFrame:
    """
    Computes predict_proba and returns a DataFrame containing the id (if available), the predicted probability, and the class label.

    Class = 1 (High probability) if the probability exceeds the threshold; otherwise 0 (Low probability).
    """
    df_probs = get_probabilities(
        df_prep=df_prep, model=model, id_col=id_col, has_id=has_id, proba_col=proba_col
    )

    df_probs[class_col] = (df_probs[proba_col] > threshold).astype(int)
    return df_probs


def classify_from_proba(
    df_probs: pd.DataFrame,
    *,
    proba_col: str = "probability",
    threshold: float = 0.5,
    class_col: str = "class",
) -> pd.DataFrame:
    """
    Assigns a classification label from a probability column.

    For each row, a value of 1 (High probability) is assigned if `proba_col` exceeds `threshold`;
    otherwise, 0 (Low probability) is assigned.
    """
    df = df_probs.copy()
    df[class_col] = (df[proba_col] > threshold).astype(int)
    return df
