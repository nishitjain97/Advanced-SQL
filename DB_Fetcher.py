import sqlite3 as sql
import pandas as pd

class DB_Fetcher:
    def __init__(
        self,
        database: str = "my_database.db"
    ) -> None:
        """
            Constructor for DB_Fetcher class
        """
        self.db = database

    def execute_query(
        self,
        query: str
    ) -> None:
        try:
            conn = sql.connect(self.db)
            cur = conn.cursor()

            cur.execute(
                query
            )

            conn.commit()
        except Exception as e:
            print(e)
        finally:
            conn.close()

    def insert_in_db(
        self,
        table: str,
        df: pd.DataFrame
    ) -> None:
        try:
            conn = sql.connect(self.db)
            cur = conn.cursor()

            query = f"""
                INSERT INTO {table}
            """
            
            col_names = []
            place_holders = []

            for column in df.columns:
                col_names.append(column)
                place_holders.append('?')

            query += f""" ({', '.join(col_names)})
                VALUES
                ({', '.join(place_holders)})
            """
            
            cur.executemany(
                query,
                df.to_records(index=False).tolist()
            )

            conn.commit()
        except Exception as e:
            print(e)
        finally:
            conn.close()

    def execute_query_to_df(
        self,
        query: str
    ) -> pd.DataFrame:
        try:
            conn = sql.connect(self.db)
            cur = conn.cursor()
            
            cur.execute(
                query
            )

            rows = cur.fetchall()

            column_names = [desc[0] for desc in cur.description]

            return pd.DataFrame(rows, columns=column_names)
        except Exception as e:
            print(e)
        finally:
            conn.close()