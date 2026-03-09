from fastapi import APIRouter
from predictive_attrition.filter_data import filter_data
from predictive_attrition.config import AppConfig
from predictive_attrition import config_paths


def predictive_attrition_entrypoint(df):
    df_ok, df_rejected = filter_data(df, cfg=cfg, id_no_model=id_no_model)

    if df_ok.empty:
        if df_rejected.empty:
            print(f"{id} incorrecto (no existe)")
        else:
            motivos = df_rejected["motivo_exclusion"]
            print("Empleado excluido")
            print(f"Motivo(s) de exclusión: {motivos}")

    empl_transform = pre.fit_transform(df_ok)
    empl_prob = get_probabilities(empl_transform, model)
    empl_pred = classify_from_proba(empl_prob, threshold=threshold)

    return empl_pred
