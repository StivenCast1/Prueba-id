import pandas as pd
import numpy as np
import warnings
import pyodbc
import time
import sys
class df_upload_sql():
    def __init__(self, data, table_name, sep:str=',', batch_size_mb:float=10.00, conection:str='DSN=impalanube', autocommit:bool=True) -> None:
        # Deshabilidat warnin
        warnings.simplefilter(action='ignore', category=FutureWarning)
        self.autocommit = autocommit
        self.conection = conection
        # Lectura de archivos
        if type(data)==pd.DataFrame:
            self.df = data.copy(deep=True)
        elif type(data) == str and data.lower().endswith('.csv'):
            try:
                self.df = pd.read_csv(data, sep=sep, dtype=str)
            except:
                ValueError('No existe el archivo csv en la ruta indicada o problemas de lectura')
        elif type(data) == str and data.lower().endswith('.xlsx'):
            self.df = pd.read_excel(data, dtype=str)
            try:
                pass
            except:
                ValueError('No existe el archivo xlsx en la ruta indicada o problemas de lectura')
        # Conversion de columnas por tipo de dato
        for column in self.df.columns:
            if not (self.df[column].astype(str).str.startswith('0').any() and ((self.df[column].astype(str).dropna().str.len()>=17)
                                                                                & (self.df[column].astype(str).dropna().str.len()<=19)).all()):
                try:
                    self.df[column] = pd.to_numeric(self.df[column])
                except:
                    pass
        # Renombre de columnas
        self.df.columns = self.df.columns.str.strip().str.lower()
        self.df.columns = self.df.columns.str.replace(' ','_').str.replace('%','procentaje').str.replace('ñ','ni')
        self.df.columns = self.df.columns.str.replace('á','a').str.replace('é','e').str.replace('í','i').str.replace('ó','o').str.replace('ú','u')
        #Extraccion de lista de tipos de datos SQL
        self.columns = []
        for _, (column, type_data) in enumerate(list(self.df.dtypes.items())):
            if type_data == 'float64':
                self.columns.append((column, 'DECIMAL(20,5)'))
            elif type_data in ('object','bool','category'):
                self.columns.append((column, 'STRING'))
            elif type_data in ('int16', 'int32', 'int64'):
                self.columns.append((column, 'BIGINT'))
            else:
                type_unknown = str(input(f'Ingrese valor desconocido par ala variable {column}: ')).lower()
                self.columns.append((column, type_unknown))
        self.table_name =  table_name
        self.batch_size_mb = batch_size_mb

    def define_queries(self):
        '''Construye queries sql con las columnas del dataframe y el nombre de la tabla.'''
        # Constructor query TRUNCATE
        self.truncate_query = f'TRUNCATE IF EXISTS {self.table_name}'
        # Constructor query DROP TABLE
        self.drop_query = f'DROP TABLE IF EXISTS {self.table_name}'
        # Constructor query CREATE TABLE
        self.create_query = f'CREATE EXTERNAL TABLE {self.table_name} ('
        self.create_query += ' '.join([val+',' if i==1 else val for column in self.columns for i, val in enumerate(column)]).rstrip(',')
        self.create_query += ') STORED AS PARQUET'
        # Constructor query INSERT INTO
        self.insert_query = f'INSERT INTO {self.table_name} ('
        self.insert_query += ' '.join([column[0]+',' for column in self.columns]).rstrip(',')+') '
        self.insert_query += 'VALUES ('+''.join([' ?,' for column in self.columns]).rstrip(',')+')'
        # Constructor query COMPUTE STATS
        self.compute_query = f'COMPUTE STATS {self.table_name}'
        # Constructor query INVALIDATE METADATA
        self.invalidate_query = f'INVALIDATE METADATA {self.table_name}'

    def execute_sql_query(self, query):
        '''Ejecuta cualquier query sql en datalake.'''
        try:
            start_time = time.time()
            with pyodbc.connect(self.conection, autocommit=self.autocommit) as con:
                cursor = con.cursor()
                cursor.execute(query)
            minutes, seconds = divmod(time.time() - start_time, 60)
            print(f'{query.split(self.table_name)[0]+self.table_name} | time: {minutes:.0f}:{seconds:02.0f} min')
        except:
            ValueError(f'Conexion no posible, asegurese que el DSN debe estar igual a como se tiene configurado el driver ODBC de Cloudera')

    def execute_many_batches(self):
        '''Inserta valores en tabla SQL en datalake en batch a partir de un dataframe.
        Utiliza conexión ODBC vía pyodbc.'''
        # se realiza split en n batches de size 10000
        row_memory_mean = self.df.memory_usage(deep=True).sum()/len(self.df)
        rows_batch = min(int((self.batch_size_mb*(1024**2))/row_memory_mean), int(250000/len(self.df.columns)))
        batches = np.array_split(self.df, len(self.df)//rows_batch+1)
        batches = [batch.replace({np.nan:None}) for batch in batches]
        # Se envía cada batch hacia datalake, el DSN debe estar igual a como se tiene configurado el driver ODBC de Cloudera.
        start_time_process = time.time()
        for i, batch in enumerate(batches):
            # Realizar proceso de carga
            with pyodbc.connect(self.conection, autocommit=self.autocommit) as con:
                start_time_batch = time.time()
                values = batch.values.tolist()
                values = [[valor.replace('|',' ') if type(valor)==str else valor if str(valor)!='nan' else None for valor in row] for row in values]
                with con.cursor() as cursor:
                    cursor.fast_executemany = True
                    cursor.executemany(self.insert_query, values)
            # Crear la barra de carga
            percent = (i + 1) / len(batches)
            bar = '█' * int(percent * 50)
            minutes_batch, seconds_batch = divmod(time.time() - start_time_batch, 60)
            minutes_process, seconds_process = divmod(time.time()-start_time_process, 60)
            memory = batch.memory_usage(deep=True).sum()*(1024**2)
            # Mostrar la barra de carga
            print(f'\r|{bar:<50}| {percent:.0%} - {i+1}/{len(batches)} - {minutes_batch:.0f}:{seconds_batch:02.0f} min - {minutes_process:.0f}:{seconds_process:02.0f} min', end='')
        print('')

    def upload(self):
        # Crear querys
        self.define_queries()
        # Query para tumbar tabla si existe
        self.execute_sql_query(self.truncate_query)
        # Query para tumbar tabla si existe
        self.execute_sql_query(self.drop_query)
        # Query para crear la tabla
        self.execute_sql_query(self.create_query)
        # Query para poblar tabla vacía y contabilizar tiempo de ejecución
        self.execute_many_batches()
        # Query para computar recursos
        self.execute_sql_query(self.compute_query)
        # Query para invalidar metadatos
        self.execute_sql_query(self.invalidate_query)

from concurrent.futures import ThreadPoolExecutor
class df_upload_sql_threads():
    def __init__(self, data, table_name, num_threads:int=4, sep:str=',', batch_size_mb:float=10.00, 
                 conection:str='DSN=impala_nube', autocommit:bool=True, key_unique:str='batch') -> None:
        # Deshabilidat warning
        warnings.simplefilter(action='ignore', category=FutureWarning)
        self.autocommit = autocommit
        self.conection = conection
        self.batch_size_mb = batch_size_mb
        # Lectura de archivos
        if type(data)==pd.DataFrame:
            self.df = data.copy()
        elif type(data) == str and data.lower().endswith('.csv'):
            try:
                self.df = pd.read_csv(data, sep=sep, dtype=str)
            except:
                ValueError('No existe el archivo csv en la ruta indicada o problemas de lectura')
        elif type(data) == str and data.lower().endswith('.xlsx'):
            self.df = pd.read_excel(data, dtype=str)
            try:
                pass
            except:
                ValueError('No existe el archivo xlsx en la ruta indicada o problemas de lectura')
        # Conversion de columnas por tipo de dato
        for column in self.df.columns:
            if not (self.df[column].astype(str).str.startswith('0').any() and ((self.df[column].astype(str).dropna().str.len()>=17)
                                                                               & (self.df[column].astype(str).dropna().str.len()<=19)).all()):
                try:
                    self.df[column] = pd.to_numeric(self.df[column])
                except:
                    pass
        # Renombre de columnas
        self.df.columns = self.df.columns.str.strip().str.lower()
        self.df.columns = self.df.columns.str.replace(' ','_').str.replace('%','procentaje').str.replace('ñ','ni')
        self.df.columns = self.df.columns.str.replace('á','a').str.replace('é','e').str.replace('í','i').str.replace('ó','o').str.replace('ú','u')
        # Inicializar parametros
        self.num_threads = num_threads
        self.table_name =  table_name
        self.batch_size_mb = batch_size_mb
        self.key_unique = key_unique
        self.upload()
    
    # Crea querys que crean la tabla
    def executor_create_table(self, table_name):
        # Constructor query TRUNCATE
        truncate_query = f'TRUNCATE IF EXISTS {table_name}'
        # Constructor query DROP TABLE
        drop_table_query = f'DROP TABLE IF EXISTS {table_name}'
        # Constructor query CREATE TABLE
        create_table_query = f'CREATE TABLE {table_name} stored as parquet as '
        return [truncate_query, drop_table_query, create_table_query]
    
    # Crea querys que crean la tabla
    def executor_create_external_table(self, table_name):
        # Constructor query TRUNCATE
        truncate_query = f'TRUNCATE IF EXISTS {table_name}'
        # Constructor query DROP TABLE
        drop_table_query = f'DROP TABLE IF EXISTS {table_name}'
        # Constructor query CREATE TABLE
        create_external_table_query = f'CREATE EXTERNAL TABLE {table_name} stored as parquet as '
        return [truncate_query, drop_table_query, create_external_table_query]

    # Crea querys que refrescan y actualizan caracteristicas de las tablas en bodega
    def executor_upgrade(self, table_name):
        # Constructor query COMPUTE STATS
        invalidate_metadata_query = f'INVALIDATE METADATA {table_name}'
        # Constructor query COMPUTE STATS
        compute_stats_query = f'COMPUTE STATS {table_name}'
        return [invalidate_metadata_query, compute_stats_query]
    
    # Ejecuta en orden los querys suministrados
    def executor_query_sql(self, compute_stats:bool=True, invalidate_metada:bool=True, refresh:bool=True):
        for table, querys in self.querys.items():
            for query in querys:
                start_time = time.time()
                with pyodbc.connect(self.conection, autocommit=self.autocommit) as con:
                    cursor = con.cursor()
                    if query.startswith('DROP') or query.startswith('TRUNCATE'):
                        cursor.execute(query)
                        minutes, seconds = divmod(time.time() - start_time, 60)
                        print(f'{query} | time: {minutes:.0f}:{seconds:02.0f} min')
                        continue
                    if query.startswith('CREATE') or query.startswith('INSERT'):
                        cursor.execute(query + ' '+ self.dict_codes[table])
                        print(f'{query} | time: {minutes:.0f}:{seconds:02.0f} min')
                        continue
                    if query.startswith('COMPUTE STATS') and compute_stats:
                        cursor.execute(query)
                        print(f'{query} | time: {minutes:.0f}:{seconds:02.0f} min')
                        continue
                    if query.startswith('INVALIDATE METADATA') and invalidate_metada:
                        cursor.execute(query)
                        print(f'{query} | time: {minutes:.0f}:{seconds:02.0f} min')
                        continue

    def upload(self):
        batches = np.array_split(self.df, self.num_threads)
        tables_names = [self.table_name+'_'+self.key_unique+str(i+1) for i in range(self.num_threads)]
        instances = [df_upload_sql(batch, tables_names[i], autocommit=self.autocommit, conection=self.conection) for i, batch in enumerate(batches)]
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            executor.map(lambda x: x.upload(), instances)
        executor.shutdown()
        self.querys = {self.table_name: self.executor_create_external_table(self.table_name)+self.executor_upgrade(self.table_name)+['DROP TABLE '+table for table in tables_names]}
        self.dict_codes = {self.table_name: ' UNION ALL '.join(['SELECT * FROM '+table for table in tables_names])}
        self.executor_query_sql()
# df_upload_sql('RECHAZOS_APROB.xlsx', 'riesgo_credit_va.base_que_existia_en_la_memoria_de_cami').upload()