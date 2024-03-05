# Import required libraries
# Do not install/import any additional libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "assignment3"
host = "127.0.0.1"


def get_open_connection():
    """
    Connect to the database and return connection object
    
    Returns:
        connection: The database connection object.
    """
    print("came into get_open_connection")
    return psycopg2.connect(f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'")

def load_data(table_name, csv_path, connection, header_file):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        connection: The database connection object.
        header_file (str): The path to where the header file is located
    """

    cursor = connection.cursor()

    # Creating the table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)
    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    create_table_query = f''' CREATE TABLE IF NOT EXISTS {table_name} ( {table_rows_formatted})'''
    cursor.execute(create_table_query)
    #print("create_table_query",create_table_query)
    connection.commit()


    # # TODO: Implement code to insert data here
    with open(csv_path) as f:
        cursor.copy_expert(f'COPY {table_name} FROM STDIN WITH CSV HEADER',f)
    connection.commit()
    
    #raise Exception("Function yet to be implemented!")



def range_partition(data_table_name, partition_table_name, num_partitions,header_file,column_to_partition, connection):
    
    """
    Use this function to partition the data in the given table using a range partitioning approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data >
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        column_to_partition (str): The column based on which we are creating the partition.
        connection: The database connection object.
    """


    # TODO: Implement code to perform range_partition here
    with open(header_file) as json_data:
        header_dict = json.load(json_data)
        
    cursor = connection.cursor()
    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    query = f'CREATE TABLE IF NOT EXISTS {partition_table_name} ({table_rows_formatted}) partition by RANGE ({column_to_partition}) ;'
    cursor.execute(query)
    connection.commit()
    
    cursor.execute("select min(created_utc),max(created_utc) from {};".format(data_table_name))
    min=cursor.fetchone()
    range_partition=math.ceil(((min[1]-min[0])/num_partitions))
    
    for i in range(0, num_partitions):
        query = "create table {} PARTITION OF {} FOR VALUES FROM ({}) TO ({}) ;".format(partition_table_name+str(i), partition_table_name, str(min[0]+(i*range_partition)), str(min[0]+(range_partition*(i+1))))
        cursor.execute(query) 
    connection.commit()

    query = f"insert into {partition_table_name} select * from {data_table_name} ;"
    cursor.execute(query)
    connection.commit()

    
    cursor.close()



def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        connection: The database connection object.
    """


    # Table to be partitioned
    cursor = connection.cursor()
    with open(header_file) as json_data:
        header_dict = json.load(json_data) 
    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    query = f'CREATE TABLE IF NOT EXISTS {partition_table_name} ({table_rows_formatted});'
    cursor.execute(query)
    connection.commit()
    cursor.execute("select min(created_utc),max(created_utc) from {};".format(data_table_name))
    min=cursor.fetchone()
    rr_partition=math.ceil(((min[1]-min[0])/num_partitions))

    
  
    for i in range(0, num_partitions):
        #query = "CREATE TABLE IF NOT EXISTS {} (CHECK ( created_utc >= {} AND created_utc < {})) INHERITS ({});".format(partition_table_name+str(i),str(min[0]+(i*rr_partition)), str(min[0]+(rr_partition*(i+1))),data_table_name)
        partition_name = f"{partition_table_name}{i}"
        query = f"CREATE TABLE {partition_name} (LIKE {data_table_name} INCLUDING ALL) INHERITS ({partition_table_name})"
        
        #print(query)
        cursor.execute(query)

   
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {data_table_name}")
        data = cursor.fetchall()

        for i, each in enumerate(data):
            value_partition= i % num_partitions
            name_partition = f"{partition_table_name}{value_partition}"
            x = ','.join(['%s'] * len(each))
            insert_query = f"INSERT INTO {name_partition} SELECT {x}"
            cursor.execute(insert_query, each)

    # trigger function
    fn_trigger = f"""
    CREATE OR REPLACE FUNCTION fn_{partition_table_name}_tr()
    RETURNS TRIGGER AS $$
    DECLARE
        ptable_name TEXT;
        ptable_count INTEGER;
        ptable_minrow INTEGER;
        ptable_min TEXT;
        i INTEGER;
    BEGIN
        FOR i IN 0..{num_partitions - 1} LOOP
            ptable_name := '{partition_table_name}' || i;
            EXECUTE 'SELECT COUNT(*) FROM ' || ptable_name INTO ptable_count;
            IF i = 0 OR ptable_count < ptable_minrow THEN
                ptable_minrow := ptable_count;
                ptable_min := ptable_name;
            END IF;
        END LOOP;
        EXECUTE 'INSERT INTO ' || ptable_min || ' VALUES ($1.*)' USING NEW;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
    with connection.cursor() as cursor:
        cursor.execute(fn_trigger)

  
    trigger_query = f"""
    CREATE TRIGGER tr_{partition_table_name}
    BEFORE INSERT ON {partition_table_name}
    FOR EACH ROW
    EXECUTE FUNCTION fn_{partition_table_name}_tr();
    """
    with connection.cursor() as cursor:
        cursor.execute(trigger_query)
