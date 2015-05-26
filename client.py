import pika
import threading
import json, time
from pika import spec
from utility import Utility
from utility import *

'''
TODO:
1. add user/passwd option for Client.__init__,like that
	credentials = pika.PlainCredentials("guest", "guest")
	conn_params = pika.ConnectionParameters("localhost", credentials = credentials)

2.
bit mandatory
This flag tells the server how to react if the messages cannot be routed to a queue.
If this flag is set, the server will return an unroutable message with a Return method.
If this flag is zero, the server silently drops the message.
The server SHOULD [implement the mandatory flag]. --> how to ???
http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish.mandatory
'''

'''
warning:
1. send message to an unkown rouing/channel/thread will be ignore

2. if we set Client.channel.confirm_delivery() , then the publish speed is slow
	separate CMDADD/CMDDEL and CMDRECOGN with different channel --> yes: cmd_channel/pub_channel
'''

class Client:
	def __init__(self, id, serverName='localhost', port=None, userName='guest', passwdName='guest',
		               vhostName='radio_guest', exchangeName='direct_logs'):
		self.cmd_msg_props = Utility.GetCmdMsgProps()
		self.thread_msg_props = Utility.GetThreadMsgProps()
		self.port = port
		self.serverName = serverName
		self.userName = userName
		self.passwdName = passwdName
		self.vhostName = vhostName
		self.exchangeName = exchangeName
		self.responseStatus = RESOPNSE_WAIT
		self.cmd_response_channel = None

		connection = self.get_connection()
		pub_channel = connection.channel()
		pub_channel.exchange_declare(exchange=self.exchangeName,
									 type='direct',
									 durable=True)
		
		'''
		declare the publish-queue and cmd-queue[only one] for every process before
		then the message can be push the queue even the server thread has not run
		''' 
		pub_channel.queue_declare(queue=id,
								  durable=True)
		pub_channel.queue_bind(exchange=self.exchangeName,
						   	   queue=id,
					   	       routing_key=id)

		pub_channel.queue_declare(queue=CMDQUEUE,
			                  durable=True)
		pub_channel.queue_bind(exchange=self.exchangeName,
						   queue=CMDQUEUE,
					   	   routing_key=CMDROUTING)

		self.pub_channel = pub_channel

		self.cmdThread = threading.Thread(target=self.cmd_response_thread, 
										  args=(id,),
										  name=CLIENTCMDTHREAD)
		self.cmdThread.start()
		
		# wait cmdThread to start up
		while True:
			if self.cmd_response_channel:
				break

	def cmd_response_thread(self, id):
		connection = self.get_connection()
		cmd_response_channel = connection.channel()

		'''
		declare the specific queue to prevent add monitor request to ignore by exchange
		set channel to rpc model
		'''
		# has declare in __init__
		#cmd_response_channel.queue_declare(queue=CMDQUEUE,
		#						   durable=True)
		
		# put channel in confirm model to confirm the message been [put into the queue]
		#cmd_response_channel.confirm_delivery()


		'''
		exclusive
			- When set to true, your queue becomes private and can only be consumed by your app.
				This is useful when you need to limit a queue to only one consumer.
		auto-delete 
			- The queue is automatically deleted when the last consumer unsubscribes.
				If you need a temporary queue used only by one consumer,combine autodelete with exclusive.
				When the consumer disconnects, the queue will be removed.
		'''
		queueName = RESPONSEQUEUE + id
		cmd_response_channel.queue_declare(queue=queueName,
								  exclusive=True,
								  auto_delete=True)
		cmd_response_channel.basic_consume(self.on_cmd_response,
                                  queue=queueName,
                                  no_ack=True)

		self.cmd_response_channel = cmd_response_channel
		# start to wait cmd from process
		print "[cmd_response_thread start_consuming]"
		cmd_response_channel.start_consuming()


	'''
	body: {'corr_id':xxx, 'request':CMDxxx, status':RESOPNSE_xxx,}
	one process can only send a cmd on turn
	'''
	def on_cmd_response(self, ch, method, props, body):
		result = json.loads(body)
		corr_id = result['corr_id']
		self.responseStatus = result['status']
		#print "response[%d]" % self.responseStatus

	# get a connection based on the connection option
	def get_connection(self):
		return Utility.getConnection(serverName=self.serverName,
									 port=self.port,
									 userName=self.userName,
									 passwdName=self.passwdName,
									 vhostName=self.vhostName)
	
	# the command entry for radio process, but the process must access the send_cmd !!!!
	# TODO:
	# how to confirm about CMDADD and CMDDEL
	def send_cmd(self, cmd):
		cmdType = cmd['action']
		id = cmd['stream_id']
		#print "send %d" % cmdType
		if cmdType == CMDADD or cmdType == CMDDEL:
			self.thread_manage(cmd)
			# wait for response
			#print "id, ", id
			while True:
				#print "in send_cmd while"
				if self.responseStatus != RESOPNSE_WAIT:
					break
		elif cmdType == CMDRECOGN:
			self.publish(cmd)
		else:
			print "[waringn] unknow cmdType"

	def thread_manage(self, cmd):
		self.responseStatus = RESOPNSE_WAIT
		id = cmd['stream_id']
		# reply to specific reply-queue
		self.pub_channel.basic_publish(exchange=self.exchangeName,
			                  routing_key=CMDROUTING,
			                  properties=pika.BasicProperties(
			                        reply_to=RESPONSEQUEUE + id,
			                        correlation_id=id,
			                        content_type="application/json"),
			                        body=json.dumps(cmd))

	def publish(self, cmd):
		'''
		publish the a recognition result for any process
		'''
		#TODO:
		# 1. add confirm or not -> not
		bodyContent = json.dumps(cmd)
		self.pub_channel.basic_publish(exchange=self.exchangeName,
						      routing_key=cmd['stream_id'],
						      properties=self.thread_msg_props,
							  body=bodyContent)
	def close(self):
		'''
		close the connection
		'''
		#TODO:
		# 1. delete all the thread, CMDQUITALL
		return


if __name__ == '__main__':
	client = Client()
	server = Server()
	#consumer.startCmdConsume()

