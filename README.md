# Project Overview
Sparkify is a startup wants to analyze the songs they have been collecting and keep track of the users activity on their music Application. As my role (Data Engineer) I will help them building their Databases.

# Project Description

## Project Structure

```
.
├── assets
│   └── images
│       └── tables.svg
├── create_tables.py
├── data
│      ├── log_data (user log folder)
│      │   ├── 2018-11-01-events.json
│      │   ├── .....
│      └── song_data (folder containing songs and artists data)
│          ├── TRAAAAW128F429D538.json
│          ├── .....
├── dl.cfg (config file)
├── etl.py
├── README.md
├── requirements.txt
├── create_buckets.py
└── tree.txt

5 directories, 107 files

```

## Tables

The design of the database schema will be based on [***Star Schema***](https://en.wikipedia.org/wiki/Star_schema), So we will have, in this project, **one** fact table and **four** dimension tables.
### Tables

![Tables Schema](/assets/images/tables.svg)

## Getting Started

### Installing Dependencies

#### Python
Follow instructions to install the latest version of python for your platform in the [Python Docs](https://docs.python.org/3/using/unix.html#getting-and-installing-the-latest-version-of-python)

#### Virtual Environment
It's recommend to work within a virtual environment whenever using Python for projects. This keeps your dependencies for each project separate and organized. Instructions for setting up a virtual environment for your platform can be found in the [Python Docs](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/)

#### PIP Dependencies
Once you have your virtual environment setup and running. Make sure you are in the right folder then install dependencies:
```
pip install -r requirements.txt
```
This will install all of the required packages within the `requirements.txt` file.

### Before Running

In the config file `dl.cfg` there is *setting* section. this section contains two fileds `PROD` and `RM_PARQUET`. The first field will determine which mode the application will be running in. The application can be running in *production* mode and *development* mode. **1** => Production | **0** => Development. The production mode will be using s3 buckets and the development mode will be using the local data in `data` folder.

The second field `RM_PARQUET` is responsible for removing or keeping the parquet folders after the processing done. **1** => remove parquet | **0** => don't remove parquet

*Note:* each time you will run the application, the parquet folders will be **removed** and then created. this option will not prevent the folders to be removed each time the application starting.

### Running Application

1. First you have to create an AWS account, please refer to this [link](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).

2. Create a new user with admin privileges, don't forget to **download** the csv file or copy the credentials of the user. [How to create user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html)

3. Put the credentials of the created user in config file `dl.cfg`. Also if you have a custom profile for aws cli, you could put your profile name in the config file.

4. Choose your buckets names (it has to be *unique*) and put them in the config file. for example, if your buckets names are `song-data`, `log-data` and `output-data`, the config file will be like that (**don't** remove the last forward slash):
```
....

[s3]
SONG_DATA_PROD=s3a://song-data/
LOG_DATA_PROD=s3a://log-data/
SONG_DATA_DEV=data/song_data/
LOG_DATA_DEV=data/log_data/
OUTPUT_DATA=s3a://output-data/

....
```

5. Create S3 Buckets. You could create them by simply running `create_bucket.py` script, this script will create and upload the content of `song_data` and `log_data` folder, so you won't need to do anything else. You could also do it from the aws console [How to create bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)

6. [Optional! If you create your buckets by yourself] Upload the content of `log_data` and `song_data` folders each one in its corresponding bucket.

7. Run `etl.py`.

### Finally 

When you finish delete your buckets.