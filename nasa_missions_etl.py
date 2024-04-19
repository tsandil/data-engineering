import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json
import etl

#Fetch the Data i.e. EXTRACT Phase
def fetch_missions_data():
    list_of_missions = [
        "STS-40",
        # "Biosatellite%20III",
        # "Biosatellite%20II",
        # "Cosmos%20782",
        # "Cosmos%20936",
        # "Cosmos%201514",
        # "Cosmos%201129",
        # "Cosmos%201667",
        # "Cosmos%201887",
        # "SpaceX-19",
        # "SpaceX-21",
        # "SpaceX-23",
        # "SpaceX-22"
    ]
    api_url = 'https://osdr.nasa.gov/'
    list_of_mission_data = []

    print(f'The Base URL is : {api_url}\n')
    print("---------------------------------------------")

    for mission in list_of_missions:
        mission_url = f'{api_url}geode-py/ws/api/mission/{mission}'

        print(f'The URL of Mission {mission} is : {mission_url}\n')
        print("---------------------------------------------")

        headers = {
            "Content-Type":"application/json",
            "Accept":"application/json"
        }

        get_mission_data = requests.get(mission_url,headers=headers)
        
        # print(get_mission_data)
        # We got <Response [200]> i.e. Request was successful....

        # Now, we convert the individual data to json format
        mission_data = get_mission_data.json()
        list_of_mission_data.append(mission_data)

    return list_of_mission_data

# Transforming the Data. i.e. TRANSFORM Phase
def transform_mission_data(list_of_mission_data):
    # list_of_dataframes = []
    # non_empty_dataframes = []
    # concatenated_dataframe = []

    def parse_json(val):
        return json.dumps(val)

    df = pd.DataFrame(list_of_mission_data)

    df['vehicle'] = df["vehicle"].apply(parse_json)
    df['people'] = df["people"].apply(parse_json)
    df['versionInfo'] = df["versionInfo"].apply(parse_json)
    df['parents'] = df["parents"].apply(parse_json)
    df['new_column'] = 'u'

    return df
    # for data in list_of_mission_data:
    #     # Since, our data consists of nested dictionaries. So, we need to normalize the data
    #     flattened_mission_data = pd.json_normalize(data=data)

    #     # Converting to DataFrames
    #     df = pd.DataFrame(flattened_mission_data,index = [0])

    #     # Adding Columns with Current Timestamp
    #     current_timestamp = datetime.now()
    #     df['Current-Time'] = current_timestamp
    #     list_of_dataframes.append(df)

    # # Creating list of non-empty dataframes using List Comprehension.
    # non_empty_dataframes = [df for df in list_of_dataframes if not df.empty]

    # if non_empty_dataframes:
    #     concatenated_dataframe = pd.concat(non_empty_dataframes, ignore_index= True)
    #     print('*********************************************************************')
    #     print(f'The data after transformation and concatenation are : \n\n{concatenated_dataframe.T}')
    #     return concatenated_dataframe


#Loading the data o Postgres 
def load_mission_data(concatenated_dataframe):
    #Creating schema in postgres named "NASA_Missions"
    schema_name = 'nasa_mission'
    table_name = 'my_nasa_mission'
    db_name = 'my_database1'

    try:        
        details ={
            'table_name' : table_name,
            'schema_name': schema_name,
            'db_name': db_name
        }
        postgres = etl.PostgresDestination(db_name=db_name)
        postgres.write_df(df=concatenated_dataframe, details=details)
        postgres.close_conn()

        print("Data Loaded Successfully")
    except Exception as e:
        print(f"Data failed to load:\n {e}")
        postgres.close_conn()


if __name__ == '__main__':
    list_of_mission_data = fetch_missions_data()
    concatenated_dataframe = transform_mission_data(list_of_mission_data)
    load_mission_data(concatenated_dataframe=concatenated_dataframe)
