import pandas as pd
from transformations import build_preprocessor
from model import load_model, get_probabilities, classify_from_proba
from config import config_paths as paths
from config import load_config

# load model
model, threshold = load_model(
    path_model=paths.PATH_MODEL,
    experiment_name=paths.experiment_name,
    run_id=paths.run_id,
)

# transformations
cfg = load_config(paths.ruleset_version)
pre = build_preprocessor(
    cfg,
    expenses_out_thrs=paths.expenses_out_thrs,
    b_survey_map_path=paths.b_survey_map,
    b_survey_imputer_path=paths.b_survey_imputer,
    salary_imputer_path=paths.salary_imputer,
    scaler_path=paths.scaler,
    model_expected_cols=model.feature_names_in_,
)


def main(df):
    empl_transform = pre.fit_transform(df)
    empl_prob = get_probabilities(empl_transform, model)
    empl_pred = classify_from_proba(empl_prob, threshold=threshold)
    return empl_pred


if __name__ == "__main__":
    # datos de entrada
    # category: ['Estructura', 'Junior', 'Senior', 'Gerente']
    # no se incluye position_name = 'DIRECTOR/A'
    row = {
        "id": 15733286,
        "office_name": "Barcelona",
        "society_name": "RSM SPAIN AUDITORES, SLP",
        "gender": "Femenino",
        "category": "Senior",
        "seniority": 3.95,
        "salary": 29500.0,
        "salary_increase": 0.0727272727272727,
        "bonus": 0.0,
        "food": 69.5833333333333,
        "bol_positive_impact": 0,
        "is_first_year": 0,
        "survey_training_technical": 3.91666666666667,  # "La formación en conocimientos técnicos útiles para mi área es buena"
        "survey_training_sofT_skills": 3.25,  # "La formación en habilidades soft (liderazgo, capacidad comercial, comunicación...) es buena"
        "survey_training_digital_skills": 3.75,  # "La formación en competencias y herramientas digitales es buena"
        "survey_open_comunication": 3.33333333333333,  # "En RSM Spain existe una comunicación bidireccional abierta y honesta"
        "survey_leaders_explain_changes": 3.16666666666667,  # "Los líderes de RSM Spain comunican adecuadamente las razones que hay detrás de los cambios relevantes que se realizan en la firma"
        "survey_internal_comm_effective": 3.08333333333333,  # "Las iniciativas de comunicación existentes son suficientes y adecuadas"
    }
    empl_result = main(pd.DataFrame([row]))
    print(empl_result)
