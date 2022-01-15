import os
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')


def add_profile_to_command(command):
    '''
    append profile option to aws command based on config file
    Parameters:
        - command: the command to be executed
    '''
    if config.get('credentials', 'PROFILE'):
            command += ' --profile {}'.format(config.get('credentials', 'PROFILE'))

    return command


def create_buckets(buckets):
    '''
    create all the buckets required
    Parameters:
        - buckets : List of buckets names
    '''
    for b in buckets:
        command = 'aws s3 mb {}'.format(b.replace('s3a', 's3'))
        os.system(add_profile_to_command(command))


def copy_data_to_buckets(data, buckets):
    '''
    remove the bucket content first
    copy the local data to the buckets
    Parameters:
        - dev: List of local data paths
        - buckets: List of buckets names
    '''
    if len(data) != len(buckets):
        raise Exception('data list and buckets list have to be the same length')

    for b in buckets:
        command = 'aws s3 rm {} --recursive'.format(b.replace('s3a', 's3'))
        os.system(add_profile_to_command(command))

    for i in range(len(data)):
        bucket = buckets[i]
        d = data[i]
        command = 'aws s3 cp {} {} --recursive'.format(d, bucket.replace('s3a', 's3'))
        os.system(add_profile_to_command(command))


if __name__ == '__main__':
    buckets = [
        config.get('s3', 'SONG_DATA_PROD'),
        config.get('s3', 'LOG_DATA_PROD'),
        config.get('s3', 'OUTPUT_DATA')
    ]
    cp_buckets = [
        config.get('s3', 'SONG_DATA_PROD'),
        config.get('s3', 'LOG_DATA_PROD')
    ]
    data = [
        config.get('s3', 'SONG_DATA_DEV'),
        config.get('s3', 'LOG_DATA_DEV')
    ]
    create_buckets(buckets)
    copy_data_to_buckets(data, cp_buckets)