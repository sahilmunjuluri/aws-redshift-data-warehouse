import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def drop_tables(cur, conn):
    """Drops all existing tables"""
    for query in drop_table_queries:
        logging.info(f"Dropping table: {query.split()[2]}")
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Creates tables as per sql_queries.py"""
    for query in create_table_queries:
        logging.info(f"Creating table: {query.split()[5]}")
        cur.execute(query)
        conn.commit()

def main():
    """Main function to set up Redshift tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect(
            host=config.get("CLUSTER", "HOST"),
            dbname=config.get("CLUSTER", "DB_NAME"),
            user=config.get("CLUSTER", "DB_USER"),
            password=config.get("CLUSTER", "DB_PASSWORD"),
            port=config.get("CLUSTER", "DB_PORT")
        )
        cur = conn.cursor()

        logging.info("Connected to Redshift successfully!")
        drop_tables(cur, conn)
        create_tables(cur, conn)
        conn.close()
        logging.info("Tables created successfully!")

    except Exception as e:
        logging.error(f"Error in creating tables: {str(e)}")

if __name__ == "__main__":
    main()
