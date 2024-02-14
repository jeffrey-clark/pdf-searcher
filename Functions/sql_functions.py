# --- SET PROJECT ROOT 

import os
import sys

# Find the project root directory dynamically
root_dir = os.path.abspath(__file__)  # Start from the current file's directory

# Traverse upwards until the .project_root file is found or until reaching the system root
while not os.path.exists(os.path.join(root_dir, '.project_root')) and root_dir != '/':
    root_dir = os.path.dirname(root_dir)

# Make sure the .project_root file is found
assert root_dir != '/', "The .project_root file was not found. Make sure it exists in your project root."

sys.path.append(root_dir)

# ---

import mysql.connector
import pandas as pd
import yaml

# def get_connection():
#     return connection.connect(host="localhost", database='journal_crawler', user="root")


def load_yaml(filepath):
    # Initialize an empty dictionary
    cfg = {}

    # Read configuration
    with open(filepath, 'r') as file:
        # Load the YAML file
        loaded_cfg = yaml.safe_load(file)

        # Check if the loaded configuration is not None
        if loaded_cfg is not None:
            cfg = loaded_cfg

    return cfg

cfg = load_yaml('config/config.yml')

def get_connection(db_name=None):
   if db_name is None:
      return mysql.connector.connect(
        host=cfg['SQL']['HOST'],
        user=cfg['SQL']['USER'],
        password=cfg['SQL']['PASSWORD']
      )
   else:
      return mysql.connector.connect(
        host=cfg['SQL']['HOST'],
        user=cfg['SQL']['USER'],
        password=cfg['SQL']['PASSWORD'],
        database=cfg['SQL']['DB_NAME']
      )

def drop_database(db_name):
   mydb = get_connection()
   my_cursor = mydb.cursor()
   sql = f"DROP DATABASE IF EXISTS {db_name}"
   my_cursor.execute(sql)
   my_cursor.close()
   mydb.close()

def confirm_database(db_name):
   db_exists = False
   mydb = get_connection()
   my_cursor = mydb.cursor()
   my_cursor.execute("SHOW DATABASES")
   for db in my_cursor:
      if db[0] == db_name:
         db_exists = True

   if not db_exists:
      my_cursor.execute(f"CREATE DATABASE {db_name};")
      print(f"Created database \"{db_name}\"")
   else:
      # print(f"Database \"{db_name}\" already exists")
       pass
   my_cursor.close()
   mydb.close()


def check_table_exists(table_name):
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    command = f'''
    SELECT * FROM information_schema.tables
    WHERE table_name = '{table_name}';
       '''

    cursor.execute(command)
    output = bool(len(cursor.fetchall()))
    cursor.close()
    mydb.close()
    return output

def drop_table(table_name):
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    # drop table if it already exists
    command = f'''
       DROP TABLE IF EXISTS {table_name};
       '''
    cursor.execute(command)
    mydb.commit()
    cursor.close()
    mydb.close()
    print(f"SQL table '{table_name}' successfully dropped")


def create_table(table_name, tuple_list, primary_key, unique_keys=None, verbose=False):
    col_type_entries = []

    # if we have AUTO_INCREMENT, we need ot declare that column as primary key upon table creation
    primary_key_on_init = ""

    for t in tuple_list:
        col_type_entries.append(f"`{t[0]}` {' '.join(t[1:])}")
        if 'AUTO_INCREMENT' in t:
            # overwrite primary key
            primary_key_name = primary_key
            primary_key_length = ""
            if type(primary_key) is tuple:
                primary_key_name = primary_key[0]
                primary_key_length = f" ({primary_key[1]})"

            if primary_key_name != t[0]:
                raise SyntaxError('Primary key must be the columns with AUTO_INCREMENT')

            primary_key_on_init = f", PRIMARY KEY (`{primary_key}`{primary_key_length}) "

    query_data = ", ".join(col_type_entries)

    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    # create the table
    command = f'''
            CREATE TABLE {table_name}(
            {query_data}{primary_key_on_init}        
            );
            '''
    if verbose:
        print(command)
    cursor.execute(command)

    # add primary key retrospectively (allowing for multi-column keys
    if primary_key_on_init == "":
        key_string_list = []
        if type(primary_key) is not list:
            primary_key = [primary_key]
        for p in primary_key:
            if (type(p) is list) or (type(p) is tuple):
                key_string_list.append(f"`{p[0]}` ({p[1]})")
            else:
                key_string_list.append(f"`{p}`")
        key_string = ", ".join(key_string_list)

        command = f'''
        ALTER TABLE `{table_name}` ADD PRIMARY KEY ({key_string});
        '''

        if verbose:
            print(command)
        cursor.execute(command)

    # add any unique keys
    if unique_keys is not None:
        if type(unique_keys) is not list:
            unique_keys = [unique_keys]
        for u in unique_keys:
            if (type(u) is list) or (type(u) is tuple):
                unique_key = ", ".join([f"`{x}`" for x in u])
            else:
                unique_key = f"`{u}`"

            command = f'''
            ALTER TABLE `{table_name}` ADD UNIQUE ({unique_key});
            '''
            if verbose:
                print(command)
            cursor.execute(command)


    cursor.close()
    mydb.close()
    print(f"Created SQL table '{table_name}")


def create_table_from_df(table_name, df, primary_key, verbose=False):

    # create string of columns for SQL command
    col_type_entries = []
    dtype_mapping = {
        'object': 'TEXT',
        'float64': 'FLOAT',
        'int64': 'INT'
    }

    tup_list = []
    for col in df.columns:
        tup_list.append((col, dtype_mapping[df.dtypes[0].name]))

    create_table(table_name, tup_list, primary_key, unique_keys=None, verbose=verbose)


def upload_to_table(table_name, df):
    df = df.fillna("NULL")

    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    # now iteratively insert
    col_tuples_list = [f"`{x}`" for x in df.columns]
    col_tuple_string = ", ".join(col_tuples_list)
    row_count = len(df)
    print(f"uploading data to SQL table: {table_name}")
    for row_id, row in df.iterrows():
        command = f'''
        INSERT IGNORE INTO `{table_name}` ({col_tuple_string})
        VALUES {tuple(row.values.tolist())};

        '''
        cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def insert(table_name, col_val_tups, verbose=False):
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    col_tuples_list = [f"`{x[0]}`" for x in col_val_tups]
    col_tuple_string = ", ".join(col_tuples_list)
    vals = []
    for x in col_val_tups:
        if x[1] is not None:
            v = f"\"{x[1]}\""
        else:
            v = "NULL"
        vals.append(v)

    command = f'''
       INSERT IGNORE INTO `{table_name}` ({col_tuple_string})
       VALUES ({", ".join(vals)});

       '''
    if verbose:
        print(command)
    cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def count_rows(table_name, where=None, verbose=False):
    output = None
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    where_string = ""
    if type(where) is list:

        equalities = []
        # flatten list if nested
        flat_list = []
        if type(where[0]) is list or type(where[0]) is tuple:
            for sub_list in where:
                flat_list += sub_list
        else:
            flat_list = where

        for i in range(0, int((len(flat_list)*0.5))):
            equalities.append(f"`{flat_list[i*2]}`=\"{flat_list[i*2+1]}\"")
        where_string = f"WHERE {' AND '.join(equalities)}"


    command = f'''
    SELECT COUNT(*) FROM `{table_name}`
    {where_string};

       '''
    if verbose:
        print(command)
    cursor.execute(command)

    output = int(cursor.fetchall()[0][0])
    cursor.close()
    mydb.close()
    return output


def get_max_in_table(table_name, column):
    # connect to the database
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()
    query = f'''
                SELECT MAX(`{column}`)
                FROM {table_name};
            '''
    cursor.execute(query)
    max_id = cursor.fetchall()[0][0]
    mydb.close()
    return max_id


def get_code_table(table_name):
    # connect to the database
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    # cursor = mydb.cursor()
    query = f'''
                SELECT *
                FROM {table_name}
                WHERE `name` IS NOT NULL;
            '''
    # cursor.execute(query)
    df = pd.read_sql(query, mydb)
    # print(cursor.fetchall())
    mydb.close()
    return df


def get_api_ids(table_name, name_list):
    # connect to the database
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()
    name_string = ", ".join([f"\"{x}\"" for x in name_list])
    query = f'''
        SELECT *
        FROM {table_name}
        where `name` in ({name_string}) 
        GROUP BY `name`
        HAVING COUNT(`name`) = 1
        ;
        '''
    cursor.execute(query)

    # store the query results and create dataframe with bool status of name list whether name matched
    result = cursor.fetchall()
    query_df = pd.DataFrame(result)
    mydb.close()
    query_df.columns = ['id', 'name']
    index_bool = pd.Series(name_list).isin(query_df['name'])
    output_df = pd.DataFrame({'name': name_list, 'match_df': index_bool})
    output_df = pd.merge(output_df, query_df, 'left', 'name')

    id_list = []
    for x in output_df['id'].values:
        if pd.isnull(x):
            id_list.append(None)
        else:
            id_list.append(int(x))

    return id_list


def get_column_names(table_name):
    # connect to the database
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()
    query = f'''
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = N'{table_name}';
        '''
    cursor.execute(query)
    result = cursor.fetchall()
    col_names = [x[3] for x in result]
    cursor.close()
    mydb.close()
    return col_names


def download(table_name):

    # connect to the database
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()
    query = f'''
           SELECT *
           FROM {table_name};
           '''
    cursor.execute(query)
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    mydb.close()

    col_names = get_column_names(table_name)
    df = pd.DataFrame(data, columns=col_names)
    return df



######################################################

def get_all_units():
    mydb = get_connection(db_name=cfg['SQL']['DB_NAME'])
    cursor = mydb.cursor()

    command = f'''
        SELECT * FROM all_units;
           '''

    cursor.execute(command)
    result = cursor.fetchall()
    cursor.close()
    mydb.close()

    df = pd.DataFrame(result, columns=['county_id', 'county', 'area_id', 'area', 'unit_id', 'unit'])
    return df


confirm_database(cfg['SQL']['DB_NAME'])