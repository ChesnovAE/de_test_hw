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


def process_scd2(ndata, sdata):
    ndata = ndata.withColumnRenamed('Osm_id', 'new_Osm_id') \
        .withColumnRenamed('Par_osm_id', 'new_Par_osm_id') \
        .withColumnRenamed('Name', 'new_Name') \
        .withColumnRenamed('Level', 'new_Level') \
        .withColumnRenamed('val_to', 'new_val_to') \
        .withColumnRenamed('val_from', 'new_val_from')
    
    merged = ndata.join(sdata, sdata.Osm_id == ndata.new_Osm_id, how='fullouter')

    merged = merged.withColumn(
        'action', 
        F.when(merged.new_Osm_id.isNull(), 'keepold')
        .when(merged.Osm_id.isNull(), 'insertnew')
        .when(merged.new_Level != merged.Level, 'update')
        .otherwise('noaction')
    )

    columns = ['Osm_id', 'Par_osm_id', 'Name', 'Level', 'val_to', 'val_from']

    # process noaction
    df_noact = merged.filter('action="noaction"').select(columns)

    # process keepold
    df_keep = merged.filter('action="keepold"').select(
        merged.Osm_id,
        merged.Par_osm_id,
        merged.Name,
        merged.Level,
        merged.val_to,
        merged.val_from
    )

    # process insertnew
    df_new = merged.filter('action="insertnew"').select(
        merged.new_Osm_id.alias('Osm_id'),
        merged.new_Par_osm_id.alias('Par_osm_id'),
        merged.new_Name.alias('Name'),
        merged.new_Level.alias('Level'),
        merged.new_val_to.alias('val_to'),
        merged.new_val_from.alias('val_from')
    )

    # process update
    df_upd = merged.filter('action="update"')

    df_upd_old = df_upd.select(
        df_upd.Osm_id,
        df_upd.Par_osm_id,
        df_upd.Name,
        df_upd.Level,
        df_upd.val_to,
        df_upd.new_val_to.alias('val_from')
    )

    df_upd_new = df_upd.select(
        df_upd.new_Osm_id.alias('Osm_id'),
        df_upd.new_Par_osm_id.alias('Par_osm_id'),
        df_upd.new_Name.alias('Name'),
        df_upd.new_Level.alias('Level'),
        df_upd.new_val_to.alias('val_to'),
        df_upd.new_val_from.alias('val_from')
    )

    full_merged = df_noact.unionAll(df_keep).unionAll(df_new).unionAll(df_upd_old).unionAll(df_upd_new)
    return full_merged


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

    def stop(self):
        self._session.stop()
