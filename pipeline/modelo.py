# %% Bibliotecas base
import pandas as pd
import numpy as np
import datetime as dt

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
from sktime.forecasting.model_selection import ForecastingOptunaSearchCV, ForecastingRandomizedSearchCV
from optuna.distributions import CategoricalDistribution, FloatDistribution, IntDistribution

# ===========
#  Métricas
# ===========
from sktime.performance_metrics.forecasting import MeanAbsoluteError, MeanAbsoluteScaledError

# %%

arima = StatsForecastAutoARIMA(sp=12)
ets = StatsForecastAutoETS(season_length=12)
ces = StatsForecastAutoCES(season_length=12)
tbats = StatsForecastAutoTBATS(seasonal_periods=12)
lgbm  = LGBMRegressor(verbosity=-1)
# catboost = CatBoostRegressor()

pipe = TransformedTargetForecaster(steps=[
    ('deseason', OptionalPassthrough(Deseasonalizer(sp=12), True)),
    # ('detrend', OptionalPassthrough(Detrender())),
    
    ('forecaster', AutoEnsembleForecaster(
        forecasters = [ arima, ets, ces, tbats], 
        regressor = lgbm
                                        )
    ),
    ('reconciler', OptimalReconciler())

])

param_grid = {
    'deseason__passthrough': CategoricalDistribution((True, False)),
    'deseason__transformer__model': CategoricalDistribution(('additive', 'multiplicative')),
    # 'forecaster__regressor': CategoricalDistribution((lgbm, catboost)),
    'reconciler': CategoricalDistribution((BottomUpReconciler(), TopdownReconciler(), OptimalReconciler()))
}


cv = ExpandingWindowSplitter(fh=range(1, 24+1), initial_window=120, step_length=1)
cv = TemporalTrainTestSplitter(test_size=24)

mae = MeanAbsoluteError()

opcv = ForecastingOptunaSearchCV(
    forecaster=pipe,
    cv=cv,
    param_grid=param_grid,
    scoring=mae,
    n_evals=10,
    error_score='raise'

)

import joblib
pmc_agg = joblib.load(r'../data/pmc_agg.joblib')
# opcv.fit(pmc_agg.dropna())

pipe.fit(pmc_agg.dropna())
pipe.predict(fh=range(1, 25))