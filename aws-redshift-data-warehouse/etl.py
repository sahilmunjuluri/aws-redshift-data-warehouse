import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_staging_tables(cur, conn):
    """Loads data from S3 to Redshift staging tables"""
    for query in copy_table_queries:
        logging.info(f"Executing: {query.split()[1]}")  # Logs COPY command name
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Data loaded successfully!")
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")

def insert_tables(cur, conn):
    """Inserts data from staging tables into final tables"""
    for query in insert_table_queries:
        logging.info(f"Inserting data into {query.split()[2]}")
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Insert successful!")
        except Exception as e:
            logging.error(f"Error inserting data: {str(e)}")

def main():
    """Main function to execute ETL"""
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
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        conn.close()
        logging.info("ETL completed successfully!")

    except Exception as e:
        logging.error(f"Error in ETL pipeline: {str(e)}")

if __name__ == "__main__":
    main()

#if __name__ == "__main__":
    #main()
