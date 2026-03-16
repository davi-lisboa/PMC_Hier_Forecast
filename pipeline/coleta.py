# %% Bibliotecas base

import pandas as pd
import numpy as np

import sidrapy as sidra
from tenacity import retry, stop_after_attempt, wait_fixed

from typing import Union, List, Literal, LiteralString

# %% get_pmc_index

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def get_pmc_index(
                tipo: str=Literal['ampliada', 'restrita']
                
                ) -> pd.DataFrame:
    
    # restrita_url: https://apisidra.ibge.gov.br/values/t/8882/n1/all/v/7169/p/all/c11046/56734/c85/all/d/v7169%205
    # ampliada_url: https://apisidra.ibge.gov.br/values/t/8883/n1/all/v/7169/p/all/c11046/56736/c85/all/d/v7169%205
    meta = {
        'restrita': {'table_code': '8882', 'classification':'11046/56734/c85/all'},
        'ampliada': {'table_code': '8883', 'classification':'11046/56736/c85/all'},
    }

    pmc_index = (
                    sidra.get_table(
                                table_code= meta[tipo]['table_code'],
                                territorial_level='1',
                                ibge_territorial_code='all',
                                variable='7169',
                                period='all',
                                classification=meta[tipo]['classification']

                            )
                .pipe(corrige_col_sidra)
                [['Mês (Código)', 'Atividades', 'Valor']]
                .assign(
                    Data = lambda df: pd.to_datetime(df['Mês (Código)'], format='%Y%m'),
                    nindice = lambda df: pd.to_numeric(df['Valor'], errors='coerce')
                )
                .groupby(['Atividades', 'Data'])[['nindice']].last()
    )
    return pmc_index
    # return meta

get_pmc_index('restrita').index.get_level_values('Atividades').unique()

# %% corrige_col_sidra

def corrige_col_sidra(df_sidra: pd.DataFrame) -> pd.DataFrame:

    df_fmt = df_sidra.copy()
    df_fmt.columns = df_fmt.iloc[0, :]
    df_fmt = df_fmt.iloc[1:].sort_index(ignore_index=True)

    return df_fmt
