#Defining imports
import pandas as pd
from urllib.request import urlopen, Request
import yaml
import json
import re
from sqlalchemy import create_engine

# QUESTION 1
def sort_numerical_list(list):
    # The given input is a list of numeric strings
    # The desired behaviour is to order the numbers ascending.
    # First convert them to integer type, otherwise the sorting will place the string '10' before '2', but we want 2 before 10
    test_list = [int(i) for i in list] 
    # then we sort the list using the implicit method sort(key=, reverse= )
    test_list.sort()
    return test_list

# QUESTION 2
def count_capital_letters(input_file):
    #First we open the target file un utf-8 encosing
    with open(input_file, encoding="utf8") as file:
        counter = 0
        #then we iterate through all characters and if it is an upper letter we increase the counter by one
        for char in file.read():
            if char.isupper():
                counter += 1
    return counter

# QUESTION 3
def parse_files(dir, file_type):
    import os
    file_list = [x for x in os.listdir(dir) if x.endswith(file_type)]
    return file_list

# QUESTION 4
def compute_drivers_performance():
    #pandas library will be used.
    #First we import both tables needed for the exercise from the csv provided
    orders_df = pd.read_csv("D:/paack/Data/orders_table.csv", sep=',')
    drivers_df = pd.read_csv("D:/paack/Data/drivers_table.csv", sep=',')

    #Before continuing we filter out duplicates
    orders_df.drop_duplicates(keep='first', inplace=True)
    drivers_df.drop_duplicates(keep='first', inplace=True) #Not really needed in this particular case

    #now we convert the time data into datetime type
    orders_df['Delivery Start'] = pd.to_datetime(orders_df['Delivery Start'])
    orders_df['Delivery End'] = pd.to_datetime(orders_df['Delivery End'])
    orders_df['Attempted time'] = pd.to_datetime(orders_df['Attempted time'])

    #lets filter out if there are deliveries with non valid attempt times (not the case)
    orders_df = orders_df[ ~orders_df.isnull() ]

    #lets compute the perfromance of each driver
    aggr_df = orders_df.groupby(['driver_id', 'Deliver date']) \
        .agg( {'Delivery Start':'min', 'Delivery End': 'max', 'id': lambda x: x.nunique()}) \
        .rename(columns={'id':'orders'}) \
        .reset_index(drop=False) 
    aggr_df['WorkHours'] = aggr_df['Delivery End'].apply(lambda x: x.hour) - aggr_df['Delivery Start'].apply(lambda x: x.hour)
    aggr_df['Performance'] = aggr_df.orders / aggr_df.WorkHours

    #Finally obtain the full river list with its performance. Drivers that did not work will be accounted for NULL performance
    merged_df = pd.merge(drivers_df, aggr_df, left_on='id', right_on='driver_id', how='left')
    merged_df.drop(columns={'driver_id'}, inplace=True)

    merged_df.to_csv("D:/paack/Data/Out/block3_out.csv", sep=',')
    return merged_df

# QUESTION 5 functions
def do_request(url, data_json = None):
    # Make the HTTP request.
    if data_json != None:
        data=bytes(json.dumps(data_json), encoding="utf-8")
        request = Request(url, data)
    else:
        request = Request(url)
    response = urlopen(request)
    assert response.code == 200

    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read())

    # Check the contents of the response.
    assert response_dict['success'] is True
    result = response_dict['result']
    print("Find Help in: {}".format(response_dict['help']))
    return result, response_dict

def get_sql_engine(server, config):
    from sqlalchemy import create_engine
    engine = None
    if(server == 'snowflake'):
        url = '''snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'''.format(**config)
        engine = create_engine(url)
    elif (server == 'other servers'):
        #neew to add its specific code.
        #For timing issues no other options have been implemnted, but a BigQuery or other servers connections can easily be coded here.
        engine = None
        
    return engine

def etl_example(server_config):
    url = 'https://opendata-ajuntament.barcelona.cat/data/api/3/action/datastore_search'
    data_json = {'resource_id': '0e3b6840-7dff-4731-a556-44fac28a7873'
                , 'offset': 0
                , 'limit' : 50000}

    # data_json = {'resource_id': '_table_metadata'}
    #Extract
    result , response_dict = do_request(url, data_json)
    result_df = pd.DataFrame(result['records'])
    a, b = result['total'], min(data_json['offset'] + data_json['limit'], result['total'])
    print("Table has {} entries. \n{} entries have been fetched".format(a, b))
    if b<a: print('''------->  Adjust 'start' and 'rows' parameters in data_json ''')
        
    #transform
    result_df = result_df[result_df.Any.apply(int) > 1950]

    #Load to a warehouse. in this case I will do it to a snowflake server
    engine = get_sql_engine('snowflake', server_config)

    #Here I would replace the existing table, it can be also a incremental load
    result_df.to_sql('Table_Name', engine, index=False, if_exists='replace') 
    return result_df


# UNIT TESTS
if __name__ == "__main__":
    print("Question 1 ...")
    assert [1, 2, 5, 8, 10] == sort_numerical_list(["5", "8", "1", "2", "10"]), "Inorrect sorting"
    print("... passed")

    print("Question 2 ...")
    input_file = "D:/paack/Data/drivers_table.csv"
    assert 1384 == count_capital_letters(input_file), "Wrong Counting"
    print("... passed")

    print("Question 3 ...")
    target_folder = "D:/paack/Data/"
    expected_result = ['drivers_summary.csv', 'drivers_table.csv', 'orders_table.csv']
    assert expected_result == parse_files(target_folder, ".csv"), "Inorrect sorting"
    print("... passed")

    print("Question 4 ...")
    df = compute_drivers_performance()
    assert 0.25 == df[df.id == 2].Performance.values, "Inorrect sorting"
    assert ['block3_out.csv'] == parse_files("D:/paack/Data/Out/", ".csv"), "Inorrect sorting"
    print("... passed")

    print("Question 5 ...")
    config = '''
    user: MY_USER
    password: MY_PWD
    account: account.eu-central-1
    warehouse: MY_WAREHOUSE
    database: MY_DATABASE
    schema: MY_SCHEMA
    '''
    config_yaml = yaml.load(config)
    df = etl_example(config_yaml)
    assert '1951' == df.Any.min(), "Inorrect sorting"
    print("... passed")