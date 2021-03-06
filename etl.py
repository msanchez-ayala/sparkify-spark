import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


### SONG DATA FUNCTIONS ###


def extract_song_data(spark, input_data):
    """
    Returns
    -------
    Spark DataFrame with all song files loaded from the input data path.

    Parameters
    ----------
    spark:

    input_data: [str] path to the data to be input with a slash at the end.
    """
    # get filepath to incoming song data
    song_data_path = input_data + 'song_data/'

    # read song data file
    df = spark.read.json('{}*/*/*/*.json'.format(song_data_path))

    return df


def songs_trans_load(df, output_data):
    """
    Transforms and loads data for songs table.

    Parameters
    ----------
    df: [spark df] full spark df of compiled song data JSON files.

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # Select columns to create songs table
    songs_table = df.selectExpr(
                        'song_id',
                        'title',
                        'artist_id',
                        'year',
                        'duration'
                    ) \
                    .drop_duplicates()

    # Write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy('year','artist_id') \
        .parquet('{}songs_table'.format(output_data), mode = 'ignore')


def artists_trans_load(df, output_data):
    """
    Transforms and loads data for artists table.

    Parameters
    ----------
    df: [spark df] full spark df of compiled song data JSON files.

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # Select columns to create artists table
    artists_table = df.selectExpr(
                        'artist_id',
                        'artist_name as name',
                        'artist_location as location',
                        'artist_latitude as latitude',
                        'artist_longitude as longitude'
                    ) \
                    .drop_duplicates()

    # Write artists table to parquet files
    artists_table.write \
        .parquet('{}artists_table'.format(output_data), mode = 'ignore')


def process_song_data(spark, input_data, output_data):
    """
    Performs ETL on song data in a spark session from a given input.
    Outputs the processed data in a specified location as parquet files.

    Parameters
    ----------
    spark:

    input_data: [str] path to the data to be input with a slash at the end.

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # Read song data files
    df = extract_song_data(spark, input_data)

    # Transform and load data for songs and artists tables
    songs_trans_load(df, output_data)
    artists_trans_load(df, output_data)


### LOG DATA FUNCTIONS ###


def extract_log_data(spark, input_data):
    """
    Returns
    -------
    Spark DataFrame with all log data files loaded from the input data path.

    Parameters
    ----------
    spark:

    input_data: [str] path to the data to be input with a slash at the end.
    """
    # get filepath to incoming log data file
    log_data_path = input_data + 'log_data/'

    # read log data file
    df = spark.read.json('{}*.json'.format(log_data_path))

    return df


def users_trans_load(df, output_data):
    """
    Transforms and loads data for users table.

    Parameters
    ----------
    df: [spark df] full spark df of compiled song data JSON files.

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # extract columns for users table
    users_table = df.selectExpr(
                        'userID as user_id',
                        'firstName as first_name',
                        'lastName as last_name',
                        'gender',
                        'level'
                    ) \
                    .drop_duplicates()

    # write users table to parquet files
    users_table.write \
        .parquet('{}users_table'.format(output_data), mode = 'ignore')


def time_trans_load(df, get_datetime, output_data):
    """
    Transforms and loads data for time table.

    Parameters
    ----------
    df: [spark df] full spark df of compiled song data JSON files.

    get_datetime: [udf] function to extract datetime object from timestamp

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # extract columns to create time table
    time_table = df.withColumn('start_time', get_datetime('ts')) \
                .withColumn('hour', hour('start_time')) \
                .withColumn('day', dayofmonth('start_time')) \
                .withColumn('week', weekofyear('start_time')) \
                .withColumn('month', month('start_time')) \
                .withColumn('year', year('start_time')) \
                .withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write \
              .parquet('{}time_table'.format(output_data), mode = 'ignore')


def songplays_trans_load(spark, input_data, df, get_datetime, output_data):
    """
    Transforms and loads data for songplays table.

    Parameters
    ----------
    spark:

    input_data: [str] path to the data to be input with a slash at the end.

    df: [spark df] full spark df of compiled song data JSON files.

    get_datetime: [udf] function to extract datetime object from timestamp

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # extract song data files to use for songplays table
    song_df = extract_song_data(spark, input_data)

    # Create aliases for the join below
    s = song_df.alias('s')
    l = df.alias('l')

    # extract cols from joined song and log datasets to create songplays table
    songplays_table = s.join(l,
        (s.artist_name == l.artist)
        & (s.title == l.song)
        & (s.duration == l.length)
    ) \
        .selectExpr(
        'l.ts as start_time',
        'l.userId as user_id',
        'l.level',
        's.song_id',
        's.artist_id',
        'l.sessionID as session_id',
        'l.location',
        'l.userAgent as user_agent'
    ) \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .withColumn('start_time', get_datetime('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .parquet(
                       '{}songplays_table'.format(output_data),
                       mode = 'ignore'
                   )


def process_log_data(spark, input_data, output_data):
    """
    Performs ETL on log data in a spark session from a given input.
    Outputs the processed data in a specified location as parquet files.

    Parameters
    ----------
    spark:

    input_data: [str] path to the data to be input with a slash at the end.

    output_data: [str] path to the data to be output with or without slash at
    the end.
    """
    # Read log data files
    df = extract_log_data(spark, input_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')


    # create timestamp column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0) \
                                      .strftime("%Y-%m-%d %H:%M:%S"))

    # Transform and load data for users, time, and songplays tables
    users_trans_load(df, output_data)
    time_trans_load(df, get_datetime, output_data)
    songplays_trans_load(spark, input_data, df, get_datetime, output_data)


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = # some bucket

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
