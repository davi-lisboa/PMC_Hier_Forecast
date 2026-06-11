# %% 
# Bibliotecas base

import os
import sys
import logging
import datetime as dt

import pandas as pd
import numpy as np

from coleta import get_pmc_index, get_pmc_pesos
from modelo import load_bundle, save_bundle
from tratamento import prettify_name, prettify_date, transform_to_yoy, aggregate_pmc, date_to_period
from reports import compare_forecasts, forecast_error

import warnings
warnings.filterwarnings('ignore', category=UserWarning)

# %% 
# Setup

HOJE = dt.date.today()

TYPE = 'restrita_sem_aberturas'
BUNDLE_PATH = r'../data/pmc_model_bundle.joblib'
FORECAST_HORIZON = range(1, 24 + 1)

# %% 
# Coleta

pmc_raw = get_pmc_index(TYPE).dropna().pipe(date_to_period, date_col='Data')

pesos_raw = get_pmc_pesos(TYPE)

pmc_agg = aggregate_pmc(pmc_raw, pesos_raw)

new_last_date = pmc_agg.index.get_level_values("Data").unique().max()

# %% 
# Load old data & model

if not os.path.exists(BUNDLE_PATH):
  print(f"Erro: bundle não encontrado em {BUNDLE_PATH}. Execute o treinamento inicial primeiro.")
  sys.exit(1)

bundle = load_bundle(BUNDLE_PATH)

old_modelo = bundle['model']
old_hist = bundle['hist']
old_preds = bundle['last_preds']
old_full_data = pd.concat([old_hist, old_preds]).sort_index()
old_last_date = bundle['last_date']

print(f"Último dado: {old_last_date.strftime('%b/%Y')}")

# %% 

there_is_new_data = new_last_date > old_last_date

if there_is_new_data:

  new_hist = pmc_agg.copy(deep=True)
  new_modelo = old_modelo.fit(new_hist)
  new_preds = new_modelo.predict(fh=FORECAST_HORIZON)
  new_full_data = pd.concat([new_hist, new_preds]).sort_index()

  save_bundle(
                modelo = new_modelo,
                hist = new_hist,
                preds = new_preds,
                last_date = new_last_date
              )

  # %% ## Reports
  
  
  forecast_error(
    new_data = prettify_name(transform_to_yoy(pmc_agg, TYPE), TYPE).query("Data > @old_last_date"),
    old_fc = prettify_name(transform_to_yoy(old_full_data, TYPE), TYPE).query("Data > @old_last_date"),
    save_path = r'../reports/forecast_error.xlsx'
  ).round(1)
  
  
  compare_forecasts(
    old_fc = prettify_name(transform_to_yoy(old_full_data, TYPE), TYPE).query('Data > @new_last_date'),
    new_fc = prettify_name(transform_to_yoy(new_full_data, TYPE), TYPE).query('Data > @new_last_date'),
    save_path = r'../reports/forecast_comparison.xlsx'
    ).round(1)
  

  
# %% 
# Exit

else:
  # logging
  print("Sem atualizações de dados. Encerrando.")
  sys.exit(0)


# %%

if __name__ == '__main__':
    pass