# %% 
# Bibliotecas base

import pandas as pd
import numpy as np
import datetime as dt

import joblib

import matplotlib.pyplot as plt
plt.style.use('seaborn-v0_8-darkgrid')

# %% 
# Módulos skitme

# ==========
#  Pipeline
# ==========
from sktime.forecasting.compose import TransformedTargetForecaster, ForecastingPipeline

# =============================
#  Modelos univariados básicos
# =============================
from sktime.forecasting.statsforecast import StatsForecastAutoARIMA, StatsForecastAutoETS, StatsForecastAutoCES, StatsForecastAutoTBATS

# ====================
#  Modelos compostos
# ====================
from sktime.forecasting.compose import AutoEnsembleForecaster

# ===============
#  Regressors
# ===============
from lightgbm import LGBMRegressor, DaskLGBMRegressor
from catboost import CatBoostRegressor

# ============================
#  Métodos de reconciliação
# ============================
from sktime.forecasting.reconcile import BottomUpReconciler, TopdownReconciler, OptimalReconciler, ReconcilerForecaster

# ===============
#  Transformers
# ===============
from sktime.transformations.series.boxcox import LogTransformer, BoxCoxTransformer
from sktime.transformations.series.detrend import Detrender, Deseasonalizer, ConditionalDeseasonalizer
from sktime.transformations.series.difference import Differencer
from sktime.transformations.compose import OptionalPassthrough

# ==================
#  Cross Validation
# ==================
from sktime.split import TemporalTrainTestSplitter, ExpandingWindowSplitter, SlidingWindowSplitter
from sktime.forecasting.model_evaluation import evaluate
from sktime.forecasting.model_selection import ForecastingOptunaSearchCV, ForecastingRandomizedSearchCV, ForecastingGridSearchCV
from optuna.distributions import CategoricalDistribution, FloatDistribution, IntDistribution

# ===========
#  Métricas
# ===========
from sktime.performance_metrics.forecasting import MeanAbsoluteError, MeanAbsoluteScaledError

# %%

def create_model():

    arima = StatsForecastAutoARIMA(sp=12)
    tbats = StatsForecastAutoTBATS(seasonal_periods=12)
    ets =  StatsForecastAutoETS(season_length=12)
    ces = StatsForecastAutoCES(season_length=12)
    # snaive = NaiveForecaster(sp=12)
    lgbm = LGBMRegressor(verbosity=-1)
    ensemble = AutoEnsembleForecaster(forecasters=[arima, tbats, ets, ces], regressor=lgbm)

    pipe = TransformedTargetForecaster(steps=[

        ('log', BoxCoxTransformer()), # Estabiliza variância
        ('deseason', Deseasonalizer(sp=12)), # Remove sazonalidade
        ('diff', Differencer()), # Remove tendência
        ('forecaster', ensemble),
        ('reconciler', OptimalReconciler())

            ])

    return pipe

# %%

def load_bundle(bundle_path: str):
    """
    Tenta carregar o modelo treinado, seu cutoff passado, a projecao passada 
    e o DataFrame combinado (real + proj) para calculos de variacao A/A do erro.
    """
    import joblib
    try:
        bundle = joblib.load(bundle_path)
        bundle_dict = dict(
            model = bundle['model'],
            last_date = bundle['last_date'],
            last_preds = bundle.get('last_preds', None),
            hist = bundle.get('hist', None)
        )

        # if hist is not None and last_preds is not None:
        #     previous_full_df = pd.concat([hist, last_preds]).sort_index()
        # else:
        #     previous_full_df = None
            
        return bundle_dict #bundle, model, last_date, last_preds, previous_full_df
    except FileNotFoundError:
        print("Modelo nao encontrado. Este sera um treinamento do zero.")
        return None #, None, None, None, None

def save_bundle(model, preds, hist, bundle_path: str):
    """
    Salva o estado completo do pipeline hierarquico e preve para a proxima iteracao.
    """
    import joblib
    new_bundle = dict(
        model = model,
        hist = hist,
        preds=preds,
        last_date = model.cutoff[0],
                    )
    joblib.dump(new_bundle, bundle_path)



def metrics(y_true, y_pred, y_train):
    import pandas as pd
    from sktime.performance_metrics.forecasting import (
                                                    MeanAbsoluteError, MeanAbsoluteScaledError,
                                                    MeanAbsolutePercentageError, MeanSquaredError
                                                    )

    mae_overall = MeanAbsoluteError()
    mae_raw = MeanAbsoluteError(multilevel='raw_values')

    rmse_raw = MeanSquaredError(square_root=True, multilevel='raw_values')

    mape_overall = MeanAbsolutePercentageError(symmetric=False)
    mape_raw = MeanAbsolutePercentageError(symmetric=False, multilevel='raw_values')

    smape_overall = MeanAbsolutePercentageError(symmetric=True)
    smape_raw = MeanAbsolutePercentageError(symmetric=True, multilevel='raw_values')

    mase_raw = MeanAbsoluteScaledError(sp=12, multilevel='raw_values')

    mae = mae_raw(y_true=y_true, y_pred=y_pred)
    rmse = rmse_raw(y_true=y_true, y_pred=y_pred)
    mape = mape_raw(y_true=y_true, y_pred=y_pred).multiply(100).round(2)
    smape = smape_raw(y_true=y_true, y_pred=y_pred).multiply(100).round(2)
    mase = mase_raw(y_true=y_true, y_pred=y_pred, y_train=y_train)

    metrics_df = pd.concat([mae, rmse, mape, smape, mase], axis=1)
    metrics_df.columns = ['MAE', 'RMSE', 'MAPE', 'sMAPE', 'MASE']

    return metrics_df


# %%
if __name__ == '__main__':
    pass