from sqlalchemy import create_engine
import psycopg2

class PostgreSQL:
    def __init__(self,cfg):
        self.host = cfg['host']
        self.port = cfg['port']
        self.username = cfg['username']
        self.password = cfg['password']
        self.database = cfg['database']
    
    # def connect(self):
    #     engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
    #     engine.connect()
    #     print('connect engine Postgresql')
    #     return engine

    def connect(self, conn_type='engine'):
        if conn_type == 'engine':
            engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
            engine.connect()
            print("Connect Engine Postgresql")
            return engine
        else:
            conn = psycopg2.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
                )
            cursor = conn.cursor()
            print("Connect Cursor Postgresql")
            return conn, cursor