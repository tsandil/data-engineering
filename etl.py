from sqlalchemy import create_engine

class PostgresDestination:

    def __init__(self, db_name):
        self.db_name = db_name
        self.db_user_name = 'tsandil'
        self.db_user_password = 'stratocaster'
        self.engine = create_engine(f"postgresql://{self.db_user_name}:{self.db_user_password}@127.0.0.1:5432/{self.db_name}")

    
    def write_df(self, df, details):
        
        table_name = details['table_name']
        schema_name = details['schema_name']

        df.to_sql(table_name, schema = schema_name, con = self.engine, if_exists="append",index=False)

    def close_conn(self):
        return self.engine.dispose()


class SchemaDriftHandle:
    pass
