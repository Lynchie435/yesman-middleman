import sqlite3
import os
import logging

logger = logging.getLogger('logger')


def get_database():
    try:
        db_path = '../database/yesman.db'

        # Ensure the directory exists
        if not os.path.exists(os.path.dirname(db_path)):
            fullpath = os.path.dirname(db_path)
            os.makedirs(fullpath)
            logger.info(f'Database created: {db_path}')

        return db_path
    except Exception as e:
        logger.error(e)

def add_usernames_to_db(data):
    try:
        logger.debug(f'Adding usernames to the database')

        connection = sqlite3.connect(get_database())
        cursor = connection.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS tbl_usernames (
                                    id INTEGER PRIMARY KEY,
                                    rank TEXT,
                                    elo TEXT,
                                    userid TEXT,
                                    username TEXT,
                                    start_date DATE,
                                    end_date DATE,
                                    is_current INTEGER
                                  )''')

        # Create a trigger to manage updates
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS trg_update_usernames
            BEFORE INSERT ON tbl_usernames
            BEGIN
                UPDATE tbl_usernames
                SET end_date = DATETIME('now'),
                    is_current = 0
                WHERE userid = NEW.userid AND is_current = 1;            
            END;
        ''')

        cursor.executemany('''
            INSERT INTO tbl_usernames (rank, elo, userid, username, start_date, end_date, is_current)
            SELECT ?, ?, ?, ?, DATETIME('now'), '9999-12-31', 1
            WHERE NOT EXISTS (
                SELECT 1
                FROM tbl_usernames
                WHERE userid = ? AND is_current = 1 AND username = ?
            )
        ''', [(row['rank'], row['elo'], row['userid'], row['username'], row['userid'], row['username']) for row in
              data])

        connection.commit()

    except sqlite3.Error as er:
        logger.error(er)

    finally:
        if connection:
            connection.close()