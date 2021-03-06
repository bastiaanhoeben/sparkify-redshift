import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from aws_config import get_cluster_endpoint


def load_staging_tables(cur, conn):
    """Copies S3 raw data into staging tables.

    Args:
        cur: Database cursor
        con: Database connection instance    
        """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Loads data from staging tables into fact and dimensional tables.
    
    Args:
        cur: Database cursor
        con: Database connection instance    
    """
   
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY=config.get('AWS', 'KEY')
    SECRET=config.get('AWS', 'SECRET')
    REGION=config.get('AWS', 'REGION')
    DWH_CLUSTER_IDENTIFIER=config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')

    DB_NAME, DB_USER, DB_PASSWORD, DB_PORT = config['DB'].values()

    host = get_cluster_endpoint(REGION, KEY, SECRET, DWH_CLUSTER_IDENTIFIER)

    conn = psycopg2.connect(f"host={host} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print("Data successfully loaded into Redshift")

    conn.close()


if __name__ == "__main__":
    main()