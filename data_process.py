import MySQLdb
import json
import threading

class Backup:
	def __init__(self, hostName='localhost', userName='test', passwdName='', dbName='test',
		         sql= "insert into result_info (access_key, stream_url, stream_id, result, timestamp,catchDate) \
					values (%s, %s, %s, %s, %s, CURRENT_DATE()) on duplicate key update id=LAST_INSERT_ID(id)"):
		self.sql = sql
		self.mdb = MySQLMgr(hostName, userName, passwdName, dbName)

	def save_one(self, data):
		if self.mdb:
			params = (data.get('access_key', 'unknow'),
				      data.get('stream_url', 'unknow'),
					  data.get('stream_id', 'unknow'),
					  json.dumps(data.get('result', {})),
					  data.get('timestamp'),)
			try:
				self.mdb.execute(self.sql, params)
				self.mdb.commit()
			except MySQLdb.Error as e:
				print e
				print "[Error][in save_one: save_one failed]"
				return False
			return True
		else:
			print "[Error][in save_one: mysql_conn is None]"
			return False

			
class MySQLMgr:
    def __init__(self, host, user, passwd, dbname):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.conn = MySQLdb.connect(host, user, passwd, dbname, charset="utf8")
        self.curs = self.conn.cursor()

    def reconnect(self):
        self.conn = MySQLdb.connect(self.host, self.user,
                self.passwd, self.dbname, charset="utf8")
        self.curs = self.conn.cursor()

    def commit(self):
        try:
            self.conn.commit();
        except (AttributeError, MySQLdb.Error):
            self.reconnect()
            try:
                self.conn.commit();
            except MySQLdb.Error:
                raise

    def execute(self, sql, params=None):
        if params:
            try:
                self.curs.execute(sql, params)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql, params)
                except MySQLdb.Error:
                    raise
        else:
            try:
                self.curs.execute(sql)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.execute(sql)
                except MySQLdb.Error:
                    raise

        return self.curs

    def executemany(self, sql, params):
        if params:
            try:
                self.curs.executemany(sql, params)
            except (AttributeError, MySQLdb.Error):
                self.reconnect()
                try:
                    self.curs.executemany(sql, params)
                except MySQLdb.Error:
                    raise

        return self.curs