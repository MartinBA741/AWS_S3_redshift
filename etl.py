import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('config read! - now connecting...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    print('connected! - now creating cursor...')
    cur = conn.cursor()
    
    print('cursor created! - now loading staging tables...')
    load_staging_tables(cur, conn)

    print('staging tables loaded! - now inserting tables...')
    insert_tables(cur, conn)

    print('tables inserted! - now closing connection...')
    conn.close()
    print('ETL done!')


if __name__ == "__main__":
    main()