import pandas as pd
import joblib
from predictive_attrition.transformations import build_preprocessor
from predictive_attrition.filter_data import load_idoposition
from predictive_attrition import config_paths
from predictive_attrition.config import load_config
from fastapi import APIRouter
from modules.simulations.application.services import predictive_attrition_entrypoint

model = joblib.load(config_paths.model_pred_fuga)
threshold = joblib.load(config_paths.threshold_pred_fuga)


cfg = load_config(config_paths.ruleset_version)

path_root = config_paths.PATH_ROOT

pre = build_preprocessor(
    cfg,
    umbral_gastos=config_paths.umbral_gastos,
    encuesta_map_path=config_paths.map_encuestas_bloque,
    imputer_enc_path=config_paths.imputer_encuestas,
    imputer_sal_path=config_paths.imputer_salario,
    scaler_path=config_paths.scaler,
    model_expected_cols=model.feature_names_in_,
)
id_no_model = load_idoposition(config_paths.PATH_ROOT)

predictive_attrition_router = APIRouter(tags=["Predictive Attrition"])


@predictive_attrition_router.post("/simulate")
def main_simulations():
    # datos de entrada
    row = {
        "id": 15714903,
        "OFICINA": "Barcelona",
        "SOCIEDAD": "RSM SPAIN SERVICIOS ADMINISTRATIVOS, SL",
        "Género": "Masculino",
        "Incorporación inorgánica": 0,
        "distance_km": 3.969664364,
        "joined_at": "2019-06-17 00:00:00",
        "left_at": None,
        "Target": 0,
        "Categoria Profesional": "Estructura",
        "Edad": 31.0,
        "Antiguedad": 7.21,
        "Promocion": 0,
        "SALARIO NUEVO": 44000.0,
        "Incremento Salarial": 0.073170732,
        "Bonus/Variable": 5000.0,
        "DIETA": 0.0,
        "final_score": 90.67,
        "Retribucion Flexible": 0,
        "Impacto positivo": 7,
        "Mis ideas y sugerencias se tienen en consideración por parte de mi supervisor directo": 4.6,
        "Mi salario es justo y el adecuado a mi nivel de responsabilidad": 4.4,
        "Considero que estoy bien retribuido en comparación con organizaciones similares": 3.4,
        "Estoy satisfecho con los beneficios sociales que recibo en RSM Spain": 4.6,
    }
    df = pd.DataFrame([row])
    empl_result = predictive_attrition_entrypoint(df)
    return empl_result.to_dict(orient="records")[0]
