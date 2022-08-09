import IPython

from src.postgres_database import PostgresDatabase

db = PostgresDatabase()

if __name__ == '__main__':
    IPython.embed()
    db.connection.close()
