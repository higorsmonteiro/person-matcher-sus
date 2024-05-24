'''
    Define the data models to store the main information on individuals 
    and linkage between different records.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''

import datetime as dt
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import DateTime, Integer, Numeric, String, Float, Sequence, ForeignKey, CheckConstraint
from sqlalchemy.exc import InternalError, IntegrityError

# ---------- datasus models ----------
class Pessoa:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'pessoa'

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, primary_key=True),
            Column("DATA_NOTIFICACAO", DateTime, nullable=True),
            Column("DATA_DIAGNOSTICO", DateTime, nullable=True),
            Column("NOME_PACIENTE", String, nullable=True),
            Column("DATA_NASCIMENTO", DateTime, nullable=True),
            Column("SEXO", String, nullable=True),
            Column("NOME_MAE", String, nullable=True),
            Column("LOGRADOURO", String, nullable=True),
            Column("LOGRADOURO_NUMERO", String, nullable=True),
            Column("BAIRRO_RESIDENCIA", String, nullable=True),
            Column("MUNICIPIO_RESIDENCIA", String, nullable=True),
            Column("CEP", String, nullable=True),
            Column("CNS", String, nullable=True),
            Column("CPF", String, nullable=True),
            Column("FONTE", String, nullable=False),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        # -- 'ID' is four columns: NU_NOTIFIC+ID_AGRAVO+DT_NOTIFIC+ID_MUNICIPIO
        self.mapping = {
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

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem
    
# ---------- MATCHING DATA MODELS ----------
class PairsLabel:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'pairs_label'

        # --> define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("FMT_ID", String, primary_key=True),
            Column("ID_1", String, nullable=False),
            Column("ID_2", String, nullable=False),
            Column("PROBA_NEGATIVO_MODELO_1", Float(5), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_2", Float(5), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_3", Float(5), nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
        )

        # -- define data mapping (could be imported if too big) - include all columns!
        self.mapping = {
            "ID_1" : "ID_1",  "ID_2" : "ID_2", 
            "FMT_ID": "FMT_ID",
            "PROBA_NEGATIVO_MODELO_1" : "PROBA_NEGATIVO_MODELO_1",
            "PROBA_NEGATIVO_MODELO_2": "PROBA_NEGATIVO_MODELO_2",
            "PROBA_NEGATIVO_MODELO_3": "PROBA_NEGATIVO_MODELO_3",  
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem