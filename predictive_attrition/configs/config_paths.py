from pathlib import Path

PATH_ROOT = Path(__file__).parent.parent

PATH_CONFIG = PATH_ROOT / 'configs'
ruleset_version = PATH_CONFIG / 'ruleset_2026_02.yaml'

PATH_TRANSFORMACIONES = PATH_ROOT / 'transformations'
expenses_out_thrs = PATH_TRANSFORMACIONES / 'expenses_outliers_threshold.pkl'
b_survey_imputer = PATH_TRANSFORMACIONES / 'b_survey_imputer.pkl'
b_survey_map = PATH_TRANSFORMACIONES / 'b_survey_map.yaml'
salary_imputer = PATH_TRANSFORMACIONES / 'salary_imputer.pkl'
scaler = PATH_TRANSFORMACIONES / 'scaler.pkl'

PATH_MODEL = PATH_ROOT / 'models'
experiment_name = 'xgboost_2'
run_id = "7be5c75d1ab742dd9638f52615e14b70"
