import pymysql


class MySQL(object):
    """MySQL Helper Function"""

    def __init__(self, host, user, passwd, db, port=3306, charset='UTF8', auto_commit=True, auto_connect=True):
        self.init_command = None
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = int(port)
        self.charset = charset.replace('-', '').upper()
        self.auto_commit = auto_commit
        self.conn = None
        self.cursor = None
        if auto_connect:
            self.connect()

    def connect(self):
        self.init_command = 'SET NAMES %s' % self.charset
        self.conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                                    charset=self.charset, init_command=self.init_command, autocommit=self.auto_commit)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def __repr__(self):
        return '%s@%s:%s/%s' % (self.user, self.host, self.port, self.db)

    def __del__(self):
        if self.cursor:
            self.conn.close()

    def affected_rows(self):
        # when using 'insert .. on duplicate key update ..'
        # 0 if an existing row is set to its current values
        # 1 if the row is inserted as a new row
        # 2 if an existing row is updated
        return self.conn.affected_rows()

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @staticmethod
    def addslashes(field):
        return field.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')

    def execute(self, query, *args, **kwargs):
        try:
            self.cursor.execute(query, *args, **kwargs)
        except:  # pymysql.OperationalError:
            self.connect()  # reconnect
            self.cursor.execute(query, *args, **kwargs)
        return

    def executemany(self, query, *args, **kwargs):
        try:
            self.cursor.executemany(query, *args, **kwargs)
        except:  # pymysql.OperationalError:
            self.connect()  # reconnect
            self.cursor.executemany(query, *args, **kwargs)
        return

    def select(self, query, *args, **kwargs):
        try:
            self.cursor.execute(query, *args, **kwargs)
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                yield row
        except:  # pymysql.OperationalError:
            self.connect()  # reconnect
            self.cursor.execute(query, *args, **kwargs)
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                yield row
        return
