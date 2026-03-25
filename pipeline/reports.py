# %% Bibliotecas base

import pandas as pd
import numpy as np

# %%

def compare_forecasts(old_fc, new_fc, save_path: str | None = None):

    import os
    import pathlib

    full_df = pd.concat([old_fc, new_fc], axis=1)
    full_df.columns = ['old_pred', 'new_pred']
    full_df['diff'] = full_df['new_pred'] - full_df['old_pred']
    full_df['diff_pct'] = (full_df['diff'] / full_df['old_pred']) * 100

    if save_path is not None:
        if os.path.exists(save_path):
            hist_diff = pd.read_excel(save_path)
            hist_diff = pd.concat([hist_diff, full_df], axis=0)
            hist_diff.to_excel(save_path)
        else:
            full_df.to_excel(save_path)

    return full_df



def forecast_error(new_data, old_fc, save_path: str | None = None):
    
    new_data_dates = new_data.index.get_level_values('Data').unique()
    fc_dates = old_fc.index.get_level_values('Data').unique()
    

    filtered_fc = old_fc.query("Data in @new_data_dates")

    error_df = pd.concat([new_data, filtered_fc], axis=1)
    error_df.columns = ['Realizado', 'Previsto']
    error_df['Erro Abs.'] = np.abs( error_df['Previsto'] - error_df['Realizado'] )
    error_df['Erro Abs. (%)'] = (error_df['Erro Abs.'] / error_df['Realizado']) * 100

    error_df = error_df.query("`Erro Abs.` != 0.000000 and `Erro Abs.`.isna() == False")

    if save_path is not None:
        if os.path.exists(save_path):
            hist_error = pd.read_excel(save_path)
            hist_error = pd.concat([hist_error, error_df], axis=0)
            hist_error.to_excel(save_path)
        else:
            error_df.to_excel(save_path)

    return error_df


# %%    
if __name__ == '__main__':
    pass