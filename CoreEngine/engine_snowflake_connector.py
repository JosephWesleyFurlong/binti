import os
import snowflake.connector as sf
from engine_keys import EngineKeys


def connect_snowflake(database_name, schema_name):
    conn = sf.connect(
            account=EngineKeys.SNOWFLAKE_ACCOUNT,
            user=EngineKeys.SNOWFLAKE_USER,
            password=EngineKeys.SNOWFLAKE_PASSWORD,
            database=database_name,
            warehouse=EngineKeys.SNOWFLAKE_WAREHOUSE,
            role=EngineKeys.SNOWFLAKE_ROLE,
            schema=schema_name,
            client_session_keep_alive=True
        )
    return conn
