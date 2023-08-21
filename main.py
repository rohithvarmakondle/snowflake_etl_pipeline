import pandas as pd
import snowflake.connector
from pyspark.sql import SparkSession
from snowflake.connector.pandas_tools import write_pandas

global sf_conn

"""
*** READ ME ***
(Step 1 for Mac or Linux only)
1.Open terminal and run "brew --version" to check if brew is installed, if not, run 
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

2.In terminal, run 'pip --version' to check if pip is installed, pip comes pre-installed with python,
to upgrade pip using python ensurepip module.
For Mac/Linux : python -m ensurepip --upgrade
For Windows : py -m ensurepip --upgrade

3.To install python pandas library, run 'pip install pandas'

4.To install snowflake connector for python, run 'brew install 'snowflake-connector-python' or 'pip install 
snowflake-connector-python' 

5.To install pandas which supports 'write_pandas', run 'brew install 'snowflake-connector-python[pandas]' or
pip install 'snowflake-connector-python[pandas]'

"""


# method to clean dataframe
def clean_data(df):
    try:
        # clean the data by removing unimportant columns
        new_df = (df.reindex(columns=['First Name', 'Last Name', 'Display Name', 'Nickname',
                                      'E-mail Address', 'Home Phone', 'Business Phone',
                                      'Mobile Phone']))
        renamed_df = new_df.rename(columns={'First Name': 'first_name',
                                            'Last Name': 'last_name',
                                            'Display Name': 'display_name',
                                            'Nickname': 'nickname',
                                            'E-mail Address': 'email_address',
                                            'Home Phone': 'home_phone',
                                            'Business Phone': 'business_phone',
                                            'Mobile Phone': 'mobile_phone'})
        # removing rows from 0 to 7 both inclusive
        cleaned_df = renamed_df.drop(range(0, 8))
        # removing rows where firstname is null from the same df
        cleaned_df.dropna(subset=['first_name'], inplace=True)
        # sorting values in dataframe using first_name
        cleaned_df.sort_values(by='first_name', inplace=True)
        # resetting index to 0 as we removed few rows in the above step which messed index
        cleaned_df.reset_index(drop=True, inplace=True)

        return cleaned_df

    except Exception as ex:
        print('Error:', ex)


# method to load data to snowflake using spark
def spark_load():
    spark = SparkSession.builder.appName('Snowflake_load').getOrCreate()

    # spark_df = spark.createDataFrame(df)
    # spark_df.createOrReplaceTempView('contacts')
    #
    # result = spark.sql('select * from contacts where first_name like "rat%"')
    return spark


# method to query snowflake (type 'quit' to exit)
def query_snowflake():
    try:
        cursor = sf_conn.cursor()
        query = input('Snowflake Query: ')

        while query != 'quit':
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            query = input('Snowflake Query: ')
    except Exception as ex:
        print('Error:'.format(ex))


# method to load dataframes to snowflake
def data_load(df):
    print('loading data to snowflake...below is the sample data')
    print(df.head().to_string())

    try:
        success, nchunks, nrows, _ = write_pandas(sf_conn, df, 'contacts',
                                                  quote_identifiers=False)
        print('success:', str(success) + ',' + 'no. of chunks:', str(nchunks) +
              ',' + 'no. of rows:', str(nrows))
        print('done...')
    except Exception as ex:
        print('Error:', ex)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        user = input('Snowflake User:')
        password = input('Snowflake Password:')

        sf_options = {
            'url': 'https://tmhwyab-ok70439.snowflakecomputing.com',
            'account': 'tmhwyab-OK70439',
            'warehouse': 'COMPUTE_WH',
            'database': 'DATAENGINEERING',
            'schema': 'ETL',
            'role': 'ACCOUNTADMIN',
            'user': user,
            'password': password
        }

        sf_conn = snowflake.connector.connect(**sf_options)
        print('Connected to Snowflake...')

        # # another way to connect to snowflake
        # sf_conn = snowflake.connector.connect(
        #     warehouse='COMPUTE_WH',
        #     account='tmhwyab-OK70439',
        #     database='DATAENGINEERING',
        #     schema='ETL',
        #     role='ACCOUNTADMIN',
        #     user= user,
        #     password= password)

        query_snowflake()

        data = pd.read_csv('/Users/rohithvarma/Downloads/contacts.csv')
        data1 = pd.read_csv('/Users/rohithvarma/Downloads/contacts1.csv')
        data2 = pd.read_csv('/Users/rohithvarma/Downloads/contacts2.csv')

        data_df = clean_data(data)
        data1_df = clean_data(data1)
        data2_df = clean_data(data2)

        frames = [data_df, data1_df, data2_df]
        final_df = pd.concat(frames)
        final_df.sort_values(by='first_name', ascending=True, inplace=True)
        final_df.reset_index(drop=True, inplace=True)
        # print(final_df.head(10).to_string())

        # loading to snowflake
        data_load(final_df)

        # query_snowflake()

        # # step to save the final dataframe to csv, if needed
        # final_df.to_csv('/Users/rohithvarma/Downloads/merged_contacts.csv')

    except Exception as e:
        print(str(e).removeprefix('250001 (08001): '))

    finally:
        try:
            sf_conn.close()
            print('Snowflake Connection closed...Good Bye!')
        except NameError:
            pass
