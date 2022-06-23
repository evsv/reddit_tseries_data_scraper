import sqlite3
from sqlite3 import Error

def init_db_file(db_file):
    """Function to initialise the database file, if it doesnt
    exist, and return a connection object to the database file.
    If the file exist, a connection to the pre-existing file
    is returned. 

    Args:
        db_file (str): Path to the database file to initialise

    Returns:
        sqlite connection: Connection object to the initialised database
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Database file {} has been created".format(db_file))
    except Error as e:
        print("Error when setting up db file: {}".format(e))
    
    return conn

def create_table(conn, create_sql_cmd, table_name):

    try:
        curs = conn.cursor()
        curs.execute(create_sql_cmd)
    except Error as e:
        print("Error creating table {}: {}".format(table_name, e))

def init_db(db_file):

    create_sub_admin_tbl = """CREATE TABLE IF NOT EXISTS submission_admin (
                                sub_id            string PRIMARY KEY,
                                subreddit         string,
                                ts_first_polled   string,
                                ts_last_polled    string,
                                sub_title         string,
                                sub_url           string); """
    
    create_sub_data_table = """CREATE TABLE IF NOT EXISTS submission_data (
                                sub_id         string,
                                ts_last_polled string,
                                num_ups        int,
                                up_ratio       int,
                                num_comms      int, 
                                is_sub_locked  string); """
    
    db_conn = init_db_file(db_file)

    create_table(db_conn, create_sub_admin_tbl, "submission_admin")
    create_table(db_conn, create_sub_data_table, "submission_data")

    db_conn.close()

if __name__ == "__main__":

    db_path = "reddit_db/reddit_sub_info.db"
    init_db(db_path)