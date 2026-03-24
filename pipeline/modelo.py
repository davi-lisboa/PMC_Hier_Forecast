# %% Bibliotecas base
import pandas as pd
import numpy as np
import datetime as dt

import joblib

import matplotlib.pyplot as plt
plt.style.use('seaborn-v0_8-darkgrid')

# %% Módulos skitme

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
    ets = StatsForecastAutoETS(season_length=12)
    ces = StatsForecastAutoCES(season_length=12)
    tbats = StatsForecastAutoTBATS(seasonal_periods=12)
    lgbm  = LGBMRegressor(verbosity=-1)
    # catboost = CatBoostRegressor()

    pipe = TransformedTargetForecaster(steps=[
        # ('deseason', OptionalPassthrough(Deseasonalizer(sp=12), True)),
        # ('detrend', OptionalPassthrough(Detrender())),
        
        ('forecaster', AutoEnsembleForecaster(
            forecasters = [ arima, ets, ces, tbats ], 
            regressor = lgbm )),
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
        pipe = bundle['model']
        last_date = bundle['meta']['last_date']
        last_preds = bundle.get('last_preds', None)
        hist = bundle.get('hist', None)
        
        if hist is not None and last_preds is not None:
            previous_full_df = pd.concat([hist, last_preds]).sort_index()
        else:
            previous_full_df = None
            
        return bundle, pipe, last_date, last_preds, previous_full_df
    except FileNotFoundError:
        print("Modelo nao encontrado. Este sera um treinamento do zero.")
        return None, None, None, None, None

def save_bundle(pipe, fh, preds, pms_agg, bundle_path: str):
    """
    Salva o estado completo do pipeline hierarquico e preve para a proxima iteracao.
    """
    import joblib
    new_bundle = dict(
        model = pipe,
        fh = fh,
        last_date = pipe.cutoff[0],
        last_preds=preds,
        hist = pms_agg
                    )
    joblib.dump(new_bundle, bundle_path)


# %%
if __name__ == '__main__':
    main()