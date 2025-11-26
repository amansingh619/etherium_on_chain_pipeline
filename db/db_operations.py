import json
from psycopg2.extras import execute_values
from utils.helpers import safe_json
from db.connection import get_conn, release_conn
from utils.logger import logger


class Database_Operations():
    """Class to handle all the DB related operations"""

    def __init__(self):
        logger.info("DB instance initialized")

    def __del__(self):
        logger.info("DB instance exited")

    def bulk_insert(self, query, rows):
        """Function for bulk insertion"""
        try:
            if not rows:
                return

            conn = get_conn()
            cur = conn.cursor()
            # Convert all rows to JSON-safe form
            safe_rows = [safe_json(r) for r in rows]

            execute_values(cur, query, safe_rows)
            conn.commit()
            release_conn(conn)
        except Exception as e:
            logger.error("Error happened while bulk insertion-> %s", e)

    def insert_blocks_data(self, rows):
        """Function to insert block data into DB"""
        self.bulk_insert("""
            INSERT INTO raw_blocks (block_number, block_timestamp, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_txs_data(self, rows):
        """Function to insert transaction data in DB"""

        self.bulk_insert("""
            INSERT INTO raw_transactions (tx_hash, block_number, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_receipts_data(self, rows):
        """Function to insert receipts data in DB"""

        self.bulk_insert("""
            INSERT INTO raw_receipts (tx_hash, block_number, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_logs_data(self, rows):
        """Function to insert logs data in DB"""
        
        self.bulk_insert("""
            INSERT INTO raw_logs (tx_hash, block_number, log_index, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)
