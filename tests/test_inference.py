from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from api.v1.main_simulations import predictive_attrition_router  # noqa


SIMULATE_PATH = "/predictive_attrition/simulate"


def create_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(
        predictive_attrition_router,
        prefix="/predictive_attrition",
        tags=["Predictive Attrition"],
    )
    return app


def parse_percent_es_to_decimal(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    s = s.replace("%", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def parse_number_es(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    s = s.replace(" ", "")

    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        parts = s.split(".")
        if len(parts) > 2:
            s = "".join(parts)
        elif (
            len(parts) == 2
            and len(parts[1]) == 3
            and parts[0].isdigit()
            and parts[1].isdigit()
        ):
            s = "".join(parts)

    try:
        return float(s)
    except ValueError:
        return None


def extract_probability(payload: Any) -> Optional[float]:
    if payload is None:
        return None

    if isinstance(payload, dict):
        for key in (
            "probability",
            "attrition_probability",
            "prediction_probability",
        ):
            if key in payload and payload[key] is not None:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    pass

        for value in payload.values():
            found = extract_probability(value)
            if found is not None:
                return found

    if isinstance(payload, list):
        for item in payload:
            found = extract_probability(item)
            if found is not None:
                return found

    return None


@pytest.mark.integration
def test_predictive_attrition_regression_report():
    input_csv = Path("tests/test_inference.csv")
    output_xlsx = Path("test_inference_results.xlsx")
    tolerance = 1e-3

    df = pd.read_csv(input_csv, sep=";", dtype=str)
    df.columns = [str(c).strip().lower() for c in df.columns]

    required_cols = {"id", "salary", "probability"}
    missing = required_cols - set(df.columns)
    if missing:
        pytest.fail(f"Faltan columnas en el CSV: {sorted(missing)}")

    df["expected_probability"] = df["probability"].apply(
        parse_percent_es_to_decimal
    )
    df["pred_probability"] = pd.NA
    df["abs_error"] = pd.NA
    df["match"] = pd.NA
    df["http_status"] = pd.NA
    df["error"] = pd.NA
    df["response_raw"] = pd.NA
    df["skipped"] = False

    with TestClient(create_test_app()) as client:
        for idx, row in df.iterrows():
            employee_id = parse_number_es(row["id"])
            new_salary = parse_number_es(row["salary"])
            expected_probability = row["expected_probability"]

            if employee_id is None or new_salary is None:
                df.at[idx, "skipped"] = True
                df.at[idx, "error"] = "Missing/invalid id or salary"
                continue

            if expected_probability is None or pd.isna(expected_probability):
                df.at[idx, "skipped"] = True
                df.at[idx, "error"] = "Missing/invalid expected probability"
                continue

            try:
                response = client.post(
                    SIMULATE_PATH,
                    params={
                        "employee_id": int(employee_id),
                        "new_salary": float(new_salary),
                    },
                )
                df.at[idx, "http_status"] = response.status_code

                if response.status_code != 200:
                    df.at[idx, "error"] = (
                        f"HTTP {response.status_code}: {response.text[:1000]}"
                    )
                    continue

                response_json = response.json()
                df.at[idx, "response_raw"] = str(response_json)[:5000]

                pred_probability = extract_probability(response_json)
                if pred_probability is None:
                    df.at[idx, "error"] = "Probability not found in response"
                    continue

                abs_error = abs(
                    float(pred_probability) - float(expected_probability)
                )

                df.at[idx, "pred_probability"] = pred_probability
                df.at[idx, "abs_error"] = abs_error
                df.at[idx, "match"] = abs_error <= tolerance

            except Exception as exc:
                df.at[idx, "error"] = repr(exc)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")

    executed = df[~df["skipped"]]
    if executed.empty:
        pytest.fail("No se ejecutó ninguna simulación")

    error_count = int(executed["error"].notna().sum())
    match_count = int(executed["match"].fillna(False).sum())
    mismatch_count = len(executed) - match_count - error_count

    print("\n--- RESUMEN ---")
    print(f"Filas ejecutadas: {len(executed)}")
    print(f"Matches: {match_count}")
    print(f"Mismatches: {mismatch_count}")
    print(f"Errores: {error_count}")

    assert error_count == 0, f"Hay {error_count} errores técnicos"
    assert (
        mismatch_count == 0
    ), f"Hay {mismatch_count} mismatches de probability"


if __name__ == "__main__":
    test_predictive_attrition_regression_report()

