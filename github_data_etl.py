import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Fetch github data of specific users --> EXTRACT(E) Phase
def fetch_github_data():
    list_of_all_data = []
    list_of_usernames = ['tsandil','psudip312','Sunnyinho']
    for username in list_of_usernames:
        api_url = 'https://api.github.com/'
        user_url = f'{api_url}users/{username}'
        repo_url= f'{api_url}users/{username}/repos/'

        print(f"The URL of the API is : --> {api_url}")
        print("\n\n---------------------------------------\n\n")
        print(f"The URL of the User is : --> {user_url}")
        print("\n\n---------------------------------------\n\n")
        print(f"The URL of the Repo is : --> {repo_url}")

        headers = {
            'Authentication':'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',
            'Content-Type':'application/vnd.github.v3+json'
        }

        get_github_data = requests.get(user_url, headers=headers)
        github_data = get_github_data.json()
        list_of_all_data.append(github_data)
    return list_of_all_data

# Transforming the data ---> TRANSFORM(T) Phase
def transform_github_data(list_of_all_data):
    list_of_dataframes = []
    for value in list_of_all_data:
        # Creating Data Frame
        dataframe = pd.DataFrame(value,index = [0])

        #Transforming the data adding column
        current_timestamp = datetime.utcnow()
        dataframe['Added-Column'] = [current_timestamp]

        # Adding the dataframe in the list of all dataframes
        list_of_dataframes.append(dataframe)

    # Using List Comprehension to add only non empty dataframes from list of dataframes 
    non_empty_dataframe_list = [dataframe for dataframe in list_of_dataframes if not dataframe.empty]
    
    # Concatenating the non-empty dataframes
    if non_empty_dataframe_list:
        concatenated_dataframe = pd.concat(list_of_dataframes,ignore_index=True)
        print(f'The data after transformation and concatenation are : \n\n{concatenated_dataframe}')
        return concatenated_dataframe
    
# Loading all the transformed data to Postgres --> Load(L) Phase
def load_github_data_to_postgres(concatenated_dataframe):
    # Create Schema in postgres first named Github
    schema_name = 'github'
    table_name = 'my_github_etl_data'

    try:
        # Creating engine from create_engine from sqlalchemy
        engine = create_engine("postgresql://tsandil:stratocaster@127.0.0.1:5432/my_database1")
        concatenated_dataframe.to_sql(table_name, schema = schema_name, con = engine, if_exists="append",index=False)

        # Closing Connection
        engine.dispose()

        print("Data Loaded Successfully")
    except Exception as e:
        print(f"Data failed to load:\n {e}")
        engine.dispose()

if __name__ == '__main__':
    list_of_all_data = fetch_github_data()
    concatenated_dataframe = transform_github_data(list_of_all_data)
    load_github_data_to_postgres(concatenated_dataframe)






