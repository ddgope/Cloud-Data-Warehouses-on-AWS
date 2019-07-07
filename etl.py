import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''
    load_staging_tables: This function will read the song and events files filesfrom Amazon S3 bucket which is object store.
    it will copy all the records and insert into syaging tables.
    Parameters: 
    cur: cursor
    con: RedShift connection information.  
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

'''
    insert_tables: This function will RedShift syaging tables and insert into Fact and Dimension tables.
    Parameters: 
    cur: cursor
    con: RedShift connection information.  
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()