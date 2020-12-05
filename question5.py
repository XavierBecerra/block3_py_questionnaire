#Defining imports
import pandas as pd
from urllib.request import urlopen, Request
import yaml
import json
import re
from sqlalchemy import create_engine

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