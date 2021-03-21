import os
import sys
import shutil
import logging
import argparse

from utils import SparkLoader, process_scd2


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser()
    setup_parser(parser)
    args = parser.parse_args()
    args.callback(args)


def process(args):
    files = list(map(lambda x: os.path.join(args.dir_path, x), os.listdir(args.dir_path)))
    loader = SparkLoader(memory=args.exec_mem, cores=args.cores, driver_memory=args.drive_mem)
    if os.path.exists(args.output):
        logging.info('Remove old parquet file')
        shutil.rmtree(args.output)
    
    logging.info('Load and process files from dir: %s', args.dir_path)
    df = None
    for file in files:
        if df is None:
            df = loader.process_csv_data(file)
        else:
            df = df.union(loader.process_csv_data(file))
    
    old_data = loader.session.read.format("jdbc"). \
    options(
         url='jdbc:postgresql://localhost:5454/',
         dbtable='test',
         user='postgres',
         password='docker',
         driver='org.postgresql.Driver'
    ).load()
    
    df = process_scd2(df, old_data)

    logging.info('Write processed data to parquet file')
    loader.write_data(df)
    
    logging.info('Write processed data to database')
    loader.write2dbase(df, table=args.table)
    
    loader.stop()


def setup_parser(parser):
    parser.add_argument(
        '--dir-path',
        '-p',
        dest='dir_path',
        help='PATH TO FOLDER THAT CONTAINS DATA',
        default='data',
        required=True,
        type=str
    )
    parser.add_argument(
        '--executor-memory',
        '-e',
        dest='exec_mem',
        help='executor spark memory size',
        default=2,
        type=int
    )
    parser.add_argument(
        '--driver-memory',
        '-d',
        dest='drive_mem',
        help='driver spark memory size',
        default=2,
        type=int
    )
    parser.add_argument(
        '--cores-count',
        '-c',
        dest='cores',
        help='max cpu spark count',
        default=2,
        type=int
    )
    parser.add_argument(
        '--output-parquet-file',
        '-o',
        dest='output',
        help='Path to output parquet file',
        default='osm.parquet',
        type=str
    )
    parser.add_argument(
        '--dbase-table',
        '-t',
        dest='table',
        help='Table in postgres  database',
        default='osm',
        type=str

    )
    parser.set_defaults(callback=process)


if __name__  == '__main__':
    main()
