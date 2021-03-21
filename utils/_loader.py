import datetime
import pandas as pd
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SparkSession, Row, functions, Column
from pyspark.sql.types import StringType 
import pyspark.sql.functions as F


def _get_last_nonnull_id(*osm_ids):
    for osm_id in osm_ids:
        if not pd.isna(osm_id):
            return osm_id
_last_nonnull_udf = F.udf(lambda osm_ids: _get_last_nonnull_id(*osm_ids), StringType())        

class SparkLoader:
    def __init__(self, memory=2, cores=2, driver_memory=2, app_name='osm'):
        self._memory = memory
        self._cores = cores
        self._driver_memory = driver_memory
        self._app_name = app_name
        self._udf_cols = list(reversed([f'ADMIN_L{i}D' for i in range(1, 11)]))
        self._session = self._create_session()
    
    @property
    def session(self):
        return self._session
    
    def _create_session(self):
        spark_conf = SparkConf().setAppName(self._app_name)
        spark_conf.set('spark.executor.memory', f'{self._memory}g')
        spark_conf.set('spark.executor.cores', f'{self._cores}')
        spark_conf.set('spark.driver.memory', f'{self._driver_memory}g')
        spark_conf.set('spark.driver.extraClassPath', '/home/ripper/postgresql-42.2.19.jar')
        spark_conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.19')
        return SparkSession.Builder().config(conf=spark_conf).getOrCreate()
    
    def process_csv_data(self, path):
        df = self._session.read.options(header=True, delimiter=';').csv(path)
        return df.withColumn('Par_osm_id', _last_nonnull_udf(F.array(self._udf_cols))) \
            .withColumnRenamed('OSM_ID', 'Osm_id') \
            .withColumnRenamed('NAME', 'Name') \
            .withColumnRenamed('ADMIN_LVL', 'Level') \
            .withColumn('val_to', F.lit(datetime.datetime.now())) \
            .withColumn('val_from', F.lit(datetime.datetime.max)) \
            .select('Osm_id', 'Par_osm_id', 'Name', 'Level', 'val_to', 'val_from')
    
    def write_data(self, df, filepath='osm.parquet', mode='append'):
        df.write.mode('append').parquet(filepath)
    
    def read_parquet(self, path='osm.parquet'):
        return self._session.read.parquet(path)
    
    def write2dbase(self, df, table='test'):
        df.write.format("jdbc"). \
            options(
                     url='jdbc:postgresql://localhost:5454/',
                     dbtable=table,
                     user='postgres',
                     password='docker',
                     driver='org.postgresql.Driver'
            ).saveAsTable(table)
    
    def read4dbase(self, df, table='test'):
        return self._session.read.format("jdbc"). \
        options(
                 url='jdbc:postgresql://localhost:5454/',
                 dbtable=table,
                 user='postgres',
                 password='docker',
                 driver='org.postgresql.Driver'
        ).load()
