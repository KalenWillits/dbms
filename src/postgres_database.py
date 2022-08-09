import os
import psycopg2
import pandas as pd
import IPython


class PostgresDatabase:
    connection = None
    queries: dict = {}

    def __init__(self):
        self.__class__.gather()
        self.connect()

    @classmethod
    def gather(cls, dirname: str = 'sql/'):
        for filename in os.listdir(dirname):
            if os.path.isdir(filename):
                cls.gather(dirname + filename + '/')
            elif filename.endswith('.sql'):
                cls.queries[filename.replace('.sql', '')] = dirname + filename

    def connect(self):
        self.__class__.connection = psycopg2.connect(
            host=os.environ.get('SOURCE'),
            database=os.environ.get('DATABASE'),
            user=os.environ.get('USERNAME'),
            password=os.environ.get('PASSWORD')
        )

    @classmethod
    def query(cls, qstring, params=None):
        if qstring in cls.queries.keys():
            with open(cls.queries[qstring]) as qfile:
                qstring = qfile.read()
        result = None

        with cls.connection.cursor() as cursor:
            try:
                cursor.execute(qstring, params)
                result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                cls.connection.commit()
            except Exception as error:
                cls.connection.rollback()
                result = error

        return result

    @classmethod
    def shell(cls):
        try:
            IPython.embed()
        finally:
            cls.connection.close()
