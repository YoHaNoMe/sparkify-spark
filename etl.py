import configparser
import os
import glob
from shutil import rmtree

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('credentials','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('credentials','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    '''
    Creates a Spark Session.
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2') \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    # spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Processes song data and creates song and artist tables
    Parameters:
        - spark        : SparkSession
        - input_data   : path to input file
        - output_data  : path to output file
    '''
    df = spark.read.json(input_data).dropDuplicates()

    # Select columns related to artist and rename some columns
    artist_df = df \
        .select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude')

    # save artist dataframe to parquet file
    artist_df.write.parquet('{}artist.parquet'.format(output_data))

    # select columns related to song
    song_df = df \
        .select('song_id', 'artist_id', 'title', 'duration', 'year')


    # save song dataframe to parquet file
    song_df.write.partitionBy('year', 'artist_id').parquet('{}song.parquet'.format(output_data))

    # create temp view for song table to access it later
    df.createOrReplaceTempView('song_table')

    print('Finish processing song data...')



def process_log_data(spark, input_data, output_data):
    '''
    Process log data and creates the user, time, and songsplay tables
    Parameters:
        - spark        : SparkSession
        - input_data   : path to input files
        - output_data  : path to store results
    '''
    df = spark.read.json(input_data)

    # filter dataframe by 'NextSong'
    df = df.filter(df.page == 'NextSong')

    # create user dataframe
    user_df = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .distinct()


    # create udf function to convert timestamp
    time_fun = udf(lambda ts : datetime.fromtimestamp(ts/1000), TimestampType())

    # create new column
    df = df.withColumn('timestamp', time_fun(df.ts))

    # extract timestamp
    df = df.withColumn('start_time', time_fun(df.ts))
    df = df.withColumn('hour', hour('timestamp'))
    df = df.withColumn('day', dayofmonth('timestamp'))
    df = df.withColumn('month', month('timestamp'))
    df = df.withColumn('year', year('timestamp'))
    df = df.withColumn('week', weekofyear('timestamp'))
    df = df.withColumn('weekday', dayofweek('timestamp'))

    # create time dataframe
    time_df = df.select('start_time', 'hour', 'day', 'month', 'year', 'week', 'weekday').distinct()

    # get song dataframe from song_table
    song_df = spark.sql(
        'SELECT * FROM song_table'
    )

    # join tables to get songplay
    songplay_df = df.join(
        song_df,
        song_df.artist_name == df.artist,
        how='inner'
    ) \
    .distinct() \
    .select(
        'start_time',
        'userId',
        'level',
        'song_id',
        'artist_id',
        'sessionId',
        'location',
        'userAgent',
        df.year,
        df.month
    ) \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('sessionId', 'session_id') \
    .withColumnRenamed('userAgent', 'user_agent') \
    .withColumn('songplay_id', monotonically_increasing_id())

    # write user parquet
    user_df.write.parquet('{}/user.parquet'.format(output_data))

    # write time parquet
    time_df.write.partitionBy('year', 'month').parquet('{}time.parquet'.format(output_data))

    # write songplay parquet
    songplay_df.write.partitionBy('year', 'month').parquet('{}songplay.parquet'.format(output_data))

    print('Finish processing log data...')


def check_and_remove_parquet_files(output_data):
    # production mode
    if config.get('settings', 'PROD') == '1':
        command = 'aws s3 rm {} --recursive'.format(output_data.replace('s3a', 's3'))
        # add profile option if it is existing in config file
        if config.get('credentials', 'PROFILE'):
            command += ' --profile {}'.format(config.get('credentials', 'PROFILE'))
        # empty bucket
        print(command)
        os.system(command)
    # development mode
    elif config.get('settings', 'PROD') == '0':
        dirs = glob.glob('*.parquet')
        for dir in dirs:
            rmtree(dir)


def main():
    '''
    1- get spark instance
    2- check if the application is going to run in production mode or development mode
    3- remove parquet files if they are exist
    4- Process song data and log data
    5- remove parquet files based on config values
    '''
    spark = create_spark_session()

    if config.get('settings', 'PROD') == '1':
        print('Running in production mode...')
        input_data_song = config.get('s3', 'SONG_DATA_PROD')
        input_data_log = config.get('s3', 'LOG_DATA_PROD')
    else:
        print('Running in development mode...')
        input_data_song = config.get('s3', 'SONG_DATA_DEV')
        input_data_log = config.get('s3', 'LOG_DATA_DEV')


    output_data = config.get('s3', 'OUTPUT_DATA')

    # remove parquet files if exist
    check_and_remove_parquet_files(output_data)

    # process data
    process_song_data(spark, input_data_song, output_data)
    process_log_data(spark, input_data_log, output_data)

    # finally remove parquet files
    if config.get('settings', 'RM_PARQUET') == '1':
        check_and_remove_parquet_files()


if __name__ == '__main__':
    main()
