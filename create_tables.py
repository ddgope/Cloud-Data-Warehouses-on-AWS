import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries,schema_queries

""" 
    craete_schema: This function will create schema in RedShift     
    Parameters: 
    cur: cursor
    conn: RedShift connection information.   
"""
def create_schema(cur, conn):
    for query in schema_queries:
        cur.execute(query)
        conn.commit()
        
""" 
    drop_tables: This function will drop all the tables     
    Parameters: 
    cur: cursor
    conn: RedShift connection information.   
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    """ 
    create_tables: This function will create all the tables in RedShift     
    Parameters: 
    cur: cursor
    conn: RedShift connection information.   
 
    """
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
    """ 
    main: This function will all the connection information from dwh.cfg file and connect to RedShift Cluster
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())    
    #print(conn)
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))        
    cur = conn.cursor()    
    create_schema(cur,conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    
if __name__ == "__main__":
    main()
    
