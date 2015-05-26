import pika
import threading


CMDQUEUE = 'cmd_queue'
PUBQUEUE = 'pub_queue'
RESPONSEQUEUE = 'response_queue_'

CMDROUTING = 'cmd_routing'
CMDCONSUMERTAG ='cmd_consumer'

CMDADD = 1
CMDDEL = 2
CMDRECOGN = 3
CMDQUITALL =  4
CMDREPORTIDS = 5

RESOPNSE_OK = 1
RESOPNSE_TIMEOUT = 2
RESOPNSE_WAIT = 3

CLIENTCMDTHREAD = 'client_cmd_thread'

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
	def getConnection(serverName='localhost', port=None, userName='guest',
					  passwdName='guest', vhostName='radio_guest'):
		conn = None
		try:
			creds = pika.PlainCredentials(userName, passwdName)
			params = pika.ConnectionParameters(serverName,
											   port=port,
										   	   virtual_host=vhostName,
										       credentials=creds)
			conn = pika.BlockingConnection(params)
		except Exception as e:
			print e

		return conn

