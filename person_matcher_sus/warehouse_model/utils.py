# -*- coding: utf-8 -*- 
'''
    Processamentos específicos.

    Autor: Higor S. Monteiro
    Email: higormonteiros@gmail.com
'''
from pathlib import Path
import pandas as pd
import numpy as np
from unidecode import unidecode

def process_sinan(df, source_name="SINAN"):
    '''
        ...
    '''
    # -- ID_SINAN: ID_AGRAVO+NU_NOTIFIC+ID_MUNICIP+DT_NOTIFIC
    notific_fmt = df["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
    id_municip_fmt = df["ID_MUNICIP"].apply(lambda x: f"{x}")
    df["ID"] = df["ID_AGRAVO"]+df["NU_NOTIFIC"]+id_municip_fmt+notific_fmt
    df["CPF"] = [ np.nan for n in range(df.shape[0]) ]
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]

    rename_dict = {
        "ID" : "ID", 
        "DT_NOTIFIC": "DATA_NOTIFICACAO",
        "NM_PACIENT": "NOME_PACIENTE", 
        "CS_SEXO": "SEXO",
        "DT_NASC": "DATA_NASCIMENTO",
        "NM_MAE_PAC": "NOME_MAE",
        "ID_MN_RESI": "MUNICIPIO_RESIDENCIA",
        "NM_BAIRRO": "BAIRRO_RESIDENCIA", 
        "NM_LOGRADO": "LOGRADOURO",
        "NU_NUMERO": "LOGRADOURO_NUMERO", 
        "NU_CEP": "CEP", 
        "ID_CNS_SUS": "CNS",
        "CPF": "CPF",
        "FONTE": "FONTE",
    }
    df = df.rename(rename_dict, axis=1)
    return df

def process_sivep(df, source_name="SIVEP"):
    '''
        ...
    '''
    df["ID"] = df["NU_NOTIFIC"].copy()
    df["DT_NOTIFIC"] = pd.to_datetime(df["DT_NOTIFIC"], errors="coerce")
    df["FONTE"] = [ source_name for n in range(df.shape[0]) ]

    rename_dict = {
        "ID" : "ID", "DT_NOTIFIC": "DATA_NOTIFICACAO",
        "NM_PACIENT": "NOME_PACIENTE", "DT_NASC": "DATA_NASCIMENTO",
        "NM_MAE_PAC": "NOME_MAE", "CO_MUN_RES": "MUNICIPIO_RESIDENCIA",
        "NM_BAIRRO": "BAIRRO_RESIDENCIA", "NM_LOGRADO": "LOGRADOURO",
        "NU_NUMERO": "LOGRADOURO_NUMERO", "NU_CEP": "CEP", 
        "NU_CNS": "CNS", "NU_CPF": "CPF", "CS_SEXO": "SEXO", "FONTE": "FONTE",
    }
    df = df.rename(rename_dict, axis=1)
    return df
