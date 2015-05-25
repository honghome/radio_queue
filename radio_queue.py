import pika
import threading
import json, time
import MySQLdb
from pika import spec
from utility import Utility
import data_process

#import logging
#logging.basicConfig()

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
	

'''
TODO:
1. add ack for subscription
3. connection option
4. how to recover thread if server down
5. how to recover the the how system if the rabbitmq-server crash  
6. add mysql model
'''
class Server:
	def __init__(self, serverName='localhost', userName='guest', passwdName='guest',
		               vhostName='radio_guest', exchangeName='direct_logs',
		               db_hostName='localhost', db_userName='test',db_passwdname='',db_dbName='test'):
		self.cmd_msg_props = Utility.GetCmdMsgProps()
		self.thread_msg_props = Utility.GetThreadMsgProps()
		self.serverName = serverName
		self.userName = userName
		self.passwdName = passwdName
		self.vhostName = vhostName
		self.exchangeName = exchangeName
		self.channelMap = {}
		self.recover()
		self.data_backup = data_process.Backup(hostName=db_hostName,
											   userName=db_userName, 
											   passwdName=db_passwdname,
											   dbName=db_dbName)

	#TODO:
	# recover based on self.channelMap or another 
	def recover(self):
		return
	def startCmdConsume(self):
		connection = self.get_connection()
		channel = connection.channel()
		channel.exchange_declare(exchange=self.exchangeName,
			                     type='direct',
			                     durable=True)
		'''
		RabbitMQ just dispatches a message when the message enters the queue.
		It doesn't look at the number of unacknowledged messages for a consumer.
		It just blindly dispatches every n-th message to the n-th consumer.

		use the basic.qos method with the prefetch_count=1 setting.
		don't dispatch a new message to a worker until it has processed and
			acknowledged the previous one
		'''

		channel.queue_declare(queue=CMDQUEUE,
			                  durable=True)
		channel.basic_qos(prefetch_count=1)

		channel.basic_consume(self.commandCallback,
							  queue=CMDQUEUE,
							  consumer_tag=CMDCONSUMERTAG)

		print "[monitor startCmdConsume]"
		try:
			channel.start_consuming()
		except KeyboardInterrupt:
			print "[in startCmdConsume]: keyboard"
		except Exception, e:
			print "[excepiton]"
			print e

		finally:
			channel.basic_cancel(consumer_tag=CMDCONSUMERTAG)
			channel.stop_consuming()
			for key, ch in self.channelMap.items():
				ch.stop_consuming()

	def commandCallback(self, ch, method, properties, body):
		bodyContent = json.loads(body)
		cmdType = bodyContent['action']
		id = bodyContent.get('stream_id')
		if cmdType == CMDADD:
			if not Utility.ThreadExist(id):
				print "[commandCallback] add a channel: %s" % id
				# after addChannel, self.channelMap[id] is not NULL
				self.addChannel(id)
			else:
				print "[commandCallback] channel has exists: %s" % id
		elif cmdType == CMDDEL:
			print "[commandCallback] delete channel thread: thread_%s" % id
			if id and (Utility.ThreadExist(id)):
				if id in self.channelMap:
					self.channelMap[id].basic_cancel(consumer_tag="consumer_" + id)
					self.channelMap[id].stop_consuming()
				else:
					print "[error] cannot find channel: %s" % id
			else:
				print "[warning] unknow thread id"

		elif cmdType == CMDREPORTIDS:
			#{'action': CMDREPORTIDS,'stream_id':[x1,x2,x3]}
			self.updateThreads(id)
		elif cmdType == CMDQUITALL:
			print "TODO quitall"

		# TODO
		# handle RESPONSE_TIMEOUT RESPONSE_ERROR ....
		'''
		no use exchange
		'''
		ch.basic_publish(exchange="",
						 routing_key=properties.reply_to,
						 properties=pika.BasicProperties(
			                          content_type="application/json"),
						 #body: {'corr_id':xxx, 'request':CMDxxx, status':RESOPNSE_xxx,}
						 body=json.dumps({'corr_id':id, 'request':cmdType, 'status':RESOPNSE_OK}))

		'''
		command channel need message acknowledgement(with the delivery_mode = 2)
		that tell the rabbitmq server that the message has been handled
		otherwise the message will still in server
		'''
		ch.basic_ack(delivery_tag=method.delivery_tag)

	# may slow
	def updateThreads(self, ids):
		for id in ids:
			if not Utility.ThreadExist(id):
				# the thread my in creating , right ??
				time.sleep(0.1)
				self.addChannel(id)

	def addChannel(self, routingKey):
		connection = self.get_connection()
		channel = connection.channel()
		self.channelMap[routingKey] = channel
		thread = threading.Thread(target = self.runThread,
								  args = (routingKey, connection, channel),
								  name = routingKey)
		thread.start()

	'''
	ch: channel object you're communicating on with RabbitMQ
	method: a method frame object that carries the consumer tag for the related subscription and the
		delivery tag for the message itself
	properties: carry optional metadata about message
	body: actual message contents[json]
	'''
	def threadCallback(self, ch, method, properties, body):
		data = json.loads(body)
		cmdType = data['action']
		if cmdType == CMDRECOGN:
			#print "[%s]receive: %s" % (threading.current_thread().getName(), data)
			self.data_backup.save_one(data)
		else:
			print "unknow cmd"
		#ch.basic_ack(delivery_tag=method.delivery_tag)

	def runThread(self, routingKey, connection, channel):
		
		id = routingKey
		'''
		self.threadCallback: will be called when a message is received for your queue
		queue=id: specific the queue you want to receive message from
		consumer_tag='consumer_' + routingKey: identifier that will dientify this subscription uniquely
			ont the AMQP channel you created with channel = connection.channel(), which is what you'd
			pass to RabbitMQ if you wanted to cancel you subscription
		no_ack=True: tell RabbitMQ you don't want to acknowledge received messages.
		'''
		channel.basic_consume(self.threadCallback,
							  queue=id,
							  consumer_tag='consumer_' + routingKey,
							  no_ack=True) # TODO: ack or not
		'''
		update self.channelMap here to
		make sure the channel can start_consuming immediately after CMDADD handler return
		TODO: maybe there is a better way
		'''
		channel.start_consuming()
		self.channelMap.pop(routingKey)
		#print "stop consume"
		#print "[thread_%s] stop consume" % threading.current_thread().getName()

	# get a connection based on the connection option
	def get_connection(self):
		return Utility.getConnection(serverName=self.serverName,
									 userName=self.userName,
									 passwdName=self.passwdName,
									 vhostName=self.vhostName)
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
	def __init__(self, id, serverName='localhost', userName='guest', passwdName='guest',
		               vhostName='radio_guest', exchangeName='direct_logs'):
		self.cmd_msg_props = Utility.GetCmdMsgProps()
		self.thread_msg_props = Utility.GetThreadMsgProps()
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

