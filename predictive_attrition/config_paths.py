from pathlib import Path

PATH_ROOT = Path(__file__).parent

PATH_CONFIG = PATH_ROOT / 'configs'
ruleset_version = PATH_CONFIG / 'ruleset_2026_01.yaml'

PATH_TRANSFORMACIONES = PATH_ROOT / 'transformations'
umbral_gastos = PATH_TRANSFORMACIONES / 'umbral_gastos.pkl'
imputer_encuestas = PATH_TRANSFORMACIONES / 'imputer_encuestas.joblib'
map_encuestas_bloque = PATH_TRANSFORMACIONES / 'encuesta_map_preguntas_bloque.yaml'
imputer_salario = PATH_TRANSFORMACIONES / 'imputer_salarios.joblib'
scaler = PATH_TRANSFORMACIONES / 'scaler.joblib'

PATH_MODEL = PATH_ROOT / 'models'
model_version = '01_01_26_model_v1'
model_pred_fuga = PATH_MODEL / model_version / 'model.pkl'
threshold_pred_fuga = PATH_MODEL / model_version / 'threshold.pkl'

PATH_RESULTS = PATH_ROOT / 'results'
file_filtered_ok = PATH_RESULTS / 'file_filtered_ok.xlsx'
file_filtered_rem = PATH_RESULTS / 'file_filtered_rem.xlsx'
file_filtered_sim = PATH_RESULTS / 'file_filtered_sim.xlsx'
file_transformations = PATH_RESULTS / 'X_transformation.xlsx'
file_probabilities = PATH_RESULTS / 'X_probabilities.xlsx'
