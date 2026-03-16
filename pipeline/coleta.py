# %% Bibliotecas base

import pandas as pd
import numpy as np

import sidrapy as sidra
from tenacity import retry, stop_after_attempt, wait_fixed

from typing import Union, List, Literal, LiteralString

# %% get_pmc_index

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def get_pmc_index(
                tipo: str=Literal['ampliada', 'ampliada_sem_aberturas', 'restrita', 'restrita_sem_aberturas']
                
                ) -> pd.DataFrame:
    
    # restrita_url: https://apisidra.ibge.gov.br/values/t/8882/n1/all/v/7169/p/all/c11046/56734/c85/all/d/v7169%205
    # ampliada_url: https://apisidra.ibge.gov.br/values/t/8883/n1/all/v/7169/p/all/c11046/56736/c85/all/d/v7169%205
    meta = {
        'restrita': {'table_code': '8882', 'classification':'11046/56734/c85/all'},
        'restrita_sem_aberturas': {'table_code': '8882', 'classification': '11046/56734/c85/2759,90671,90672,90673,103155,103156,103157,103158'},
        
        'ampliada': {'table_code': '8883', 'classification':'11046/56736/c85/all'},
        'ampliada_sem_aberturas': {'table_code': '8883', 'classification':'11046/56736/c85/2759,2762,56741,90671,90672,90673,103155,103156,103157,103158,103159'}
    }

    pmc_hier_dict = {
    'Combustíveis e lubrificantes': 
        '1. Combustíveis e lubrificantes',

    'Hipermercados, supermercados, produtos alimentícios, bebidas e fumo':
        '2. Hipermercados, supermercados, produtos alimentícios, bebidas e fumo',

    'Hipermercados e supermercados':
        '2.1 Hipermercados e supermercados',

    'Tecidos, vestuário e calçados':
        '3. Tecidos, vestuário e calçados',

    'Móveis e eletrodomésticos':
        '4. Móveis e eletrodomésticos',

    'Móveis':
        '4.1 Móveis',

    'Eletrodomésticos':
        '4.2 Eletrodomésticos',

    'Artigos farmacêuticos, médicos, ortopédicos, de perfumaria e cosméticos':
        '5. Artigos farmacêuticos, médicos, ortopédicos, de perfumaria e cosméticos',

    'Livros, jornais, revistas e papelaria':
        '6. Livros, jornais, revistas e papelaria',

    'Equipamentos e materiais para escritório, informática e comunicação':
        '7. Equipamentos e materiais para escritório, informática e comunicação',

    'Outros artigos de uso pessoal e doméstico':
        '8. Outros artigos de uso pessoal e doméstico',

    'Veículos, motocicletas, partes e peças':
        '9. Veículos, motocicletas, partes e peças',

    'Material de construção':
        '10. Material de construção',

    'Atacado especializado em produtos alimentícios, bebidas e fumo':
        '11. Atacado especializado em produtos alimentícios, bebidas e fumo'
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
                    nindice = lambda df: pd.to_numeric(df['Valor'], errors='coerce'),
                    Atividades = lambda df: df['Atividades'].map(pmc_hier_dict)
                )
                .groupby(['Atividades', 'Data'])[['nindice']].last()
    )
    return pmc_index

# get_pmc_index('restrita_sem_aberturas')

# %% corrige_col_sidra

def corrige_col_sidra(df_sidra: pd.DataFrame) -> pd.DataFrame:

    df_fmt = df_sidra.copy()
    df_fmt.columns = df_fmt.iloc[0, :]
    df_fmt = df_fmt.iloc[1:].sort_index(ignore_index=True)

    return df_fmt

# %% get_pmc_pesos

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def get_pmc_pesos(
        tipo: str=Literal['ampliada', 'ampliada_sem_aberturas', 'restrita', 'restrita_sem_aberturas']
            ) -> pd.DataFrame:

    path = r'../data/2023_Participações das Atividades PMC_ Pesos_volume base 2022.xlsx'

    pesos = pd.read_excel(path, sheet_name='Atividades', skipfooter=1)

    match tipo:
        case 'ampliada_sem_aberturas':
            pesos = pesos[['Atividades', 'Pesos_vol_varejo_ampliado (Base 2022)']]
            pesos.columns = ['Atividades', 'Pesos']

        case 'restrita_sem_aberturas':
            pesos = pesos[['Atividades', 'Pesos_vol_varejo (Base 2022)']]
            pesos.columns = ['Atividades', 'Pesos']
            pesos.dropna(inplace=True)

        case 'ampliada': 
            raise NotImplementedError
        
        case 'restrita':
            raise NotImplementedError
        

    pesos['Pesos'] = pesos['Pesos'] * 100 

    
    return pesos


# get_pmc_pesos('restrita_sem_aberturas')
