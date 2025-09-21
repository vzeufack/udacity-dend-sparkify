import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes COPY commands to load data from S3 into staging tables in the Redshift cluster.

    Parameters:
    cur (psycopg2.cursor): Cursor object used to execute SQL statements.
    conn (psycopg2.connection): Connection object used to commit transactions.
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Query completed')


def insert_tables(cur, conn):
    """
    Executes INSERT statements to transform and load data from staging tables into final fact and dimension tables.

    Parameters:
    cur (psycopg2.cursor): Cursor object used to execute SQL statements.
    conn (psycopg2.connection): Connection object used to commit transactions.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Query completed')


def main():
    """
    Orchestrates the ETL pipeline by connecting to the Redshift cluster,
    loading data into staging tables, and inserting it into final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()