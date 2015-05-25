import pika
import threading

class Utility:
	@staticmethod
	def GetThreadMsgProps():
		# properties for message
		thread_msg_props = pika.BasicProperties()
		thread_msg_props.content_type = "application/json"
		return thread_msg_props

	@staticmethod
	def GetCmdMsgProps():
		'''
		for a message that's in flight inside Rabbit to survive a crash, the message must
			- Have its delivery mode option set to 2 (persistent)
			- Be published into a durable exchange -> channel.exchange_declare(durable=True)
			- Arrive in a durable queue -> channel.queue_declare(durable=True)
		'''
		cmd_msg_props = pika.BasicProperties()
		cmd_msg_props.content_type = "application/json"
		#cmd_msg_props.delivery_mode = 2 # make message persistent
		return cmd_msg_props

	@staticmethod
	def ThreadExist(threadName):
		for thread in threading.enumerate():
			if thread.name == threadName:
				return True
		return False

	@staticmethod
	def getConnection(serverName='localhost', userName='guest',
					  passwdName='guest', vhostName='radio_guest'):
		conn = None
		try:
			creds = pika.PlainCredentials(userName, passwdName)
			params = pika.ConnectionParameters(serverName,
										   	   virtual_host=vhostName,
										       credentials=creds)
			conn = pika.BlockingConnection(params)
		except Exception as e:
			print e

		return conn

