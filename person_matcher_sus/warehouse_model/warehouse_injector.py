'''
    Define the warehouse to store relevant data for IST disease.

    The focus of this warehouse is to store relevant data for CEVEPI that are not 
    directly accesible with SQL queries in official databases. Examples include
    the ones from SINAN online (arboviruses) or SIVEP-GRIPE.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''

# -- lib
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from pathlib import Path

# -- import the warehouse class
from lente_ist import WarehouseIST
from lente_ist.utils import select_folder
from lente_ist.warehouse_model.utils import process_siclom, process_sinan

class InjectorIST:
    '''
        Interface to handle the injection of different data to the warehouse.
        
        Args:
        -----
            source_data_location:
                String. Absolute path to the folder containing source data for injection.
            warehouse_location:
                String. Absolute path to the folder containing (or that will contain) the database with the injected data.
            warehouse_name:
                String. Filename of the database.
                
        Attributes:
        -----------
            expected_files:
                List of Strings.
    '''
    def __init__(self, source_data_location, warehouse_location, warehouse_name):
        self._expected_files = [
            Path("AIDSANET.DBF"),
            Path("AIDSCNET.DBF"),
            Path("SICLOM.xlsx")
            # SIM?
        ]
        self.source_guide = {
            "AIDSANET": "SINAN", "AIDSCNET": "SINAN", "SICLOM": "SICLOM",
        }

        self.basefolder = Path(source_data_location)
        self.warehouse_location = Path(warehouse_location)
        self.warehouse_name = Path(warehouse_name)
        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"

        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()

    @property
    def expected_files(self):
        return self._expected_files

    @expected_files.setter
    def expected_files(self, v):
        raise Exception("Immutable property.")

    def drop_tables(self):
        '''
            If it is necessary to delete all current stored data before injection, then
            this function will perform this operation.
        '''
        table_names = list(self.warehouse.tables.keys())
        for tb_name in table_names:
            self.warehouse.delete_table(tb_name, is_sure=True, authkey="###!Y!.")
        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()

    def inject_data(self):
        '''

        '''
        for current_file in self._expected_files:
            source_name = current_file.stem
            extension = current_file.suffix.lower()

            # -- check whether data exists in the source folder
            if self.basefolder.joinpath(current_file).exists():
                if extension=='.dbf':
                    data_df = Dbf5(self.basefolder.joinpath(current_file), codec='latin').to_dataframe()
                elif extension==".xlsx":
                    data_df = pd.read_excel(self.basefolder.joinpath(current_file))

            if self.source_guide[source_name]=="SINAN":
                data_df = process_sinan(data_df, source_name=source_name)
            elif self.source_guide[source_name]=="SICLOM":
                data_df = process_siclom(data_df, source_name=source_name)

            # -- check whether a ID already exists in the db
            min_year, max_year = data_df["DT_NOTIFIC"].min().year, data_df["DT_NOTIFIC"].max().year
            list_of_ids = []
            try:
                for current_year in np.arange(min_year, max_year+1, 1):
                    list_of_ids += [ pd.DataFrame(self.warehouse.query_id('pessoa', current_year)) ]
                list_of_ids = pd.concat(list_of_ids)
                if list_of_ids.shape[0]>0:
                    list_of_ids = list_of_ids["ID"]
                # -- remove from the dbf the records already present in the database
                data_df = data_df[~data_df["ID"].isin(list_of_ids)].copy()
            except:
                print("no records to check duplicated IDs.")
            
            self.warehouse.insert('pessoa', data_df, batchsize=200, verbose=False)
            if source_name=="AIDSANET":
                self.warehouse.insert('sinan_aids_adulto_info', data_df, batchsize=200, verbose=False)
            elif source_name=="AIDSCNET":
                self.warehouse.insert('sinan_aids_crianca_info', data_df, batchsize=200, verbose=False)
            elif source_name=="SICLOM":
                self.warehouse.insert('siclom_info', data_df, batchsize=200, verbose=False)
            print("done.")


