import requests
from collections import defaultdict
from CoreEngine import engine_snowflake_connector as esc

class Engine:
    def __init__(self, url, snowflake_db, snowflake_schema, snowflake_table, SB_mapping,
                 operation_name, token, key_identifier=""):
        self.url = url
        self.snowflake_db = snowflake_db
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table
        self.SB_mapping = SB_mapping
        self.operation_name = operation_name
        self.key_identifier = key_identifier
        self.token = token

    def request_and_process_data(self, page_number=1):
        try:
            # data = requests.get(self.url)

            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json',
                'User-Agent': 'PostmanRuntime/7.29.2'
            }

            params = {
                "page[number]": page_number,
                "tz": "utc"
            }

            data = requests.get(self.url, headers=headers, params=params)
            metadata = dict()
            print("Request Status Code", data.status_code)
            if data.status_code == 200:
                print("Data from URL->", data.json())
                data = data.json()
                metadata = data["meta"]
            # data = {
            #   "data": [
            #     {
            #       "id": "911026",
            #       "type": "agency-workers",
            #       "attributes": {
            #         "created-at": "2022-03-22T16:07:23.936Z",
            #         "updated-at": "2024-05-24T01:29:22.663Z",
            #         "name": "FH Licensing",
            #         "email": "iiiiii@crossnore.org",
            #         "supervisors": [],
            #         "managers-of-supervisors": [],
            #         "agency-role": "approvals_placements_admin",
            #         "role-for-permissions": {
            #           "id": 9,
            #           "name": "Approvals/Placements Worker w/ All Agency Families Access"
            #         },
            #         "external-identifier": "",
            #         "sign-in-count": 2
            #       },
            #       "relationships": {
            #         "agency-admin-assignment": {
            #           "data": {
            #             "id": "11126",
            #             "type": "agency-admin-assignments"
            #           }
            #         }
            #       }
            #     }]
            # }

            BV_mapping = defaultdict(dict)
            for key in self.SB_mapping.keys():
                key_relations = key.split(".")
                key_relations_length = len(key_relations)
                for each_data in data["data"]:
                    if not each_data or each_data is None:
                        continue
                    id = each_data["id"]
                    flag = True
                    for relation_index in range(key_relations_length):
                        if not each_data or key_relations[relation_index] not in each_data:
                            flag = False
                            break
                        each_data = each_data[key_relations[relation_index]]
                    data_dict = {
                        key: each_data if flag else "NA"
                    }
                    BV_mapping[int(id)].update(data_dict)
            return BV_mapping, metadata
        except Exception as e:
            print("Error-", e)
            return None, None

    def add_data_to_snowflake(self, page_number=1):
        data, meta_data = self.request_and_process_data(page_number=page_number)
        if self.operation_name == "insert":
            self.insert_data_to_snowflake(data)
        elif self.operation_name == "update_insert":
            self.update_data_to_snowflake(data)
        return meta_data

    def insert_data_to_snowflake(self, data):
        conn = esc.connect_snowflake(self.snowflake_db, self.snowflake_schema)
        cur = conn.cursor()
        column_names = ", ".join([self.SB_mapping[each_val] for each_val in data[list(data.keys())[0]].keys()])
        column_data_list = []
        for each_value in data.values():
            column_data = []
            for key, value in each_value.items():
                if isinstance(value, list):
                    value = ",".join(map(str, value))
                if isinstance(value, str):
                    value=value.replace("'", "")
                column_data.append(f'\'{value}\'')
            column_data = f"({','.join(column_data)})"
            column_data_list.append(column_data)
        sql_column_data = ", ".join(column_data_list)
        sql_query = (f'INSERT INTO "{self.snowflake_db}"."{self.snowflake_schema}"."{self.snowflake_table}" '
                     f'({column_names}) VALUES {sql_column_data}')
        cur.execute(sql_query)

    def update_data_to_snowflake(self, data):
        data_keys = list(data.keys())
        conn = esc.connect_snowflake(self.snowflake_db, self.snowflake_schema)
        cur = conn.cursor()
        sql_query = f'SELECT {self.key_identifier} FROM "{self.snowflake_db}"."{self.snowflake_schema}"."{self.snowflake_table}" where {self.key_identifier} in ({",".join(map(str, data_keys))})'
        sql_data = cur.execute(sql_query)
        sql_data = sql_data.fetchall()
        sql_data = [each_data[0] for each_data in sql_data]
        # update data
        for each_id in sql_data:
            data_to_be_updated = data[int(each_id)]
            sql_query = f'UPDATE "{self.snowflake_db}"."{self.snowflake_schema}"."{self.snowflake_table}" SET '
            for key, value in data_to_be_updated.items():
                if isinstance(value, list):
                    value = ",".join(map(str, value))
                sql_query += f'{self.SB_mapping[key]} = \'{value}\','
            sql_query = sql_query[:-1]
            sql_query += f' WHERE {self.key_identifier} = {each_id}'
            cur.execute(sql_query)
        remaining_keys = list(set(map(int, data_keys)) - set(map(int, sql_data)))
        insertion_data = defaultdict(dict)
        for each_key in remaining_keys:
            insertion_data[each_key] = data[each_key]
        if len(insertion_data) > 0:
            self.insert_data_to_snowflake(insertion_data)

    def start_engine(self):
        print("Engine Started")
        current_page = 1
        while True:
            data = self.add_data_to_snowflake(page_number=current_page)
            total_pages = data["total_pages"]
            current_page = data["current_page"]
            if current_page < total_pages:
                current_page = current_page + 1
            else:
                break
        print("Engine Stopped")

