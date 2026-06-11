# %% Bibliotecas base

import pandas as pd
import numpy as np

# %%

def compare_forecasts(old_fc, new_fc, save_path: str | None = None):
    
    import os
    import pandas as pd
    import numpy as np
    import datetime as dt

    HOJE = dt.date.today()

    full_df = pd.concat([old_fc, new_fc], axis=1)
    full_df.columns = ['old_pred', 'new_pred']
    full_df['Diferença (p.p)'] = full_df['new_pred'] - full_df['old_pred']
    # full_df['diff_pct'] = (full_df['Diferença (p.p)'] / full_df['old_pred']) * 100

    full_df['Data_Update'] = HOJE

    full_df = full_df.sort_index().dropna()

    if save_path is not None:
        if os.path.exists(save_path):
            hist_diff = pd.read_excel(save_path)
            hist_diff = pd.concat([hist_diff.set_index(old_fc.index.names), full_df], axis=0)
            hist_diff.to_excel(save_path)
        else:
            full_df.to_excel(save_path)

    return full_df



def forecast_error(new_data, old_fc, save_path: str | None = None):

    import os
    import pandas as pd
    import numpy as np
    import datetime as dt

    HOJE = dt.date.today()
    
    new_data_dates = new_data.index.get_level_values('Data').unique()
    fc_dates = old_fc.index.get_level_values('Data').unique()
    

    filtered_fc = old_fc.query("Data in @new_data_dates")

    error_df = pd.concat([new_data, filtered_fc], axis=1)
    error_df.columns = ['Realizado', 'Previsto']
    error_df['Erro Abs.'] = np.abs( error_df['Previsto'] - error_df['Realizado'] )
    # error_df['Erro Abs. (%)'] = (error_df['Erro Abs.'] / error_df['Realizado']) * 100
    # error_df['Erro Abs. (%, Simétrico)'] = (np.abs(error_df['Previsto'] - error_df['Realizado']) / ((np.abs(error_df['Realizado']) + np.abs(error_df['Previsto'])) / 2)) * 100


    error_df = error_df.query("`Erro Abs.` != 0.000000 and `Erro Abs.`.isna() == False")
    error_df['Data_Update'] = HOJE

    if save_path is not None:
        if os.path.exists(save_path):
            hist_error = pd.read_excel(save_path)
            hist_error = pd.concat([hist_error.set_index(new_data.index.names), error_df], axis=0)
            hist_error.to_excel(save_path)
        else:
            error_df.to_excel(save_path)

    return error_df


# %%    
if __name__ == '__main__':
    pass