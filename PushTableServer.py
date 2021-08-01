import sqlalchemy
import time
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus

class pushtable:
    sever = None
    database = None
    pushtablename = None
    ptb = None

    def __init__(self, ptb, sever, database, pushtablename):
        self.ptb=ptb
        self.sever = sever
        self.database=database
        self.pushtablename= pushtablename


    def sqlcol(self):

        dtypedict = {}
        for i, j in zip(self.ptb.columns, self.ptb.dtypes):
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

            if "category" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Float(precision=1, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        return dtypedict

    def replace(self):
        conn = "Driver={SQL Server};Server="+self.sever+";Database="+self.database+";Trusted_Connection=yes;"
        #print(conn)
        quoted = quote_plus(conn)
        new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
        engine = create_engine(new_con)

        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            print("FUNC call")
            if executemany:
                cursor.fast_executemany = True

        self.ptb.to_sql(self.pushtablename, engine, if_exists='replace', chunksize=100, index=False, dtype=self.sqlcol())

    def append(self):
        conn = "Driver={SQL Server};Server="+self.sever+";Database="+self.database+";Trusted_Connection=yes;"
        #print(conn)
        quoted = quote_plus(conn)
        new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
        engine = create_engine(new_con)

        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            print("FUNC call")
            if executemany:
                cursor.fast_executemany = True
        self.ptb.to_sql(self.pushtablename, engine, if_exists='append', chunksize=100, index=False, dtype=self.sqlcol())



