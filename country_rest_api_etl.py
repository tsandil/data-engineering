import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Fetch Data From Rest API. (Extract Phase)

def fetch_data():
    list_of_all_data = []
    list_of_countries = ['Nepal','India','Pakistan','USA','Italy','Switzerland','Australia','Canada']
    
    for country in list_of_countries:
        base_url = 'https://restcountries.com/'
        data_url = f'{base_url}v3.1/name/{country}'

        print(f'The Base URL is : --> {base_url}\n')
        print("========================================\n")
        print(f'The URL which has data is : --> {data_url}\n')

        headers = {
            
            'Accept': 'application/json'
        }
        # Using the GET Method
        get_country_data = requests.get(data_url,headers=headers)
        country_data = get_country_data.json()
        list_of_all_data.append(country_data)
    return list_of_all_data

# Transforming the data to dataframes and more... (Transform Phase)
def transform_data(list_of_all_data):
    list_of_dataframes = []
    non_empty_dataframes = []
    
    concatenated_dataframe = []
    for data in list_of_all_data:
        
        # Since,in the json data the name field is inside a dictionary and contains a dictionary(nested). So, we flatten it
        flattened_data = {
            'common_name': data[0]['name']['common'],
            'official_name': data[0]['name']['official'],
            'tld': data[0]['tld'][0],
            'cca2': data[0]['cca2'],
            'ccn3': data[0]['ccn3'],
            'cca3': data[0]['cca3'],
            # 'cioc': data[0]['cioc'],
            'independent': data[0]['independent'],
            'status': data[0]['status'],
            'unMember': data[0]['unMember'],
            # 'currency_name': data[0]['currencies']['CHF']['name'],
            # 'currency_symbol': data[0]['currencies']['CHF']['symbol'],
            'capital': data[0]['capital'][0],
            'region': data[0]['region'],
            'subregion': data[0]['subregion'],
            'population': data[0]['population'],
            'timezones': data[0]['timezones'][0],
            'area': data[0]['area'],
            # 'demonym_female': data[0]['demonyms']['fra']['f'],
            # 'demonym_male': data[0]['demonyms']['fra']['m'],
            'flag_emoji': data[0]['flag'],
            'google_maps_url': data[0]['maps']['googleMaps'],
            'openstreet_maps_url': data[0]['maps']['openStreetMaps'],
            'start_of_week': data[0]['startOfWeek'],
            'latitude': data[0]['capitalInfo']['latlng'][0],
            'longitude': data[0]['capitalInfo']['latlng'][1],
            # 'postal_code_format': data[0]['postalCode']['format'],
            # 'postal_code_regex': data[0]['postalCode']['regex'],
        }

        df = pd.DataFrame(flattened_data, index = [0])
        
        # Transforming the data and adding columns
        current_timestamp = datetime.utcnow()
        df["Added_Column"] = [current_timestamp]

        list_of_dataframes.append(df)
        pass

    non_empty_dataframes = [df for df in list_of_dataframes if not df.empty]

    if non_empty_dataframes:
        concatenated_dataframe = pd.concat(non_empty_dataframes,ignore_index=True)
        print(f'The data after transformation and concatenation are : \n\n{concatenated_dataframe}')
        return concatenated_dataframe        
    pass

# Loading the data into PostgreSQL (User : tsandil   Schema : countries   table-name : country_data ) (Loading Phase)
def load_data(concatenated_dataframe):

    schema_name = 'countries'
    table_name = "country_data"

    try:
        # Create Engine and use pandas' .to_sql()  method
        engine = create_engine("postgresql://tsandil:stratocaster@127.0.0.1:5432/my_database2")
        concatenated_dataframe.to_sql(table_name, schema = schema_name, con = engine, if_exists="append",index=True)
        print("Data Loaded Successfully")

        engine.dispose()
    except Exception as e:
        print(f"Data failed to load:\n {e}")
        engine.dispose()    
    pass
if __name__ == '__main__':
    list_of_all_data = fetch_data()
    concatenated_dataframe = transform_data(list_of_all_data)
    load_data(concatenated_dataframe)