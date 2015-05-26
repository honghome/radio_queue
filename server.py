import pika
import threading
import json, time
from pika import spec
from utility import Utility
from utility import *
import data_process

'''
TODO:
1. add ack for subscription
3. connection option
4. how to recover thread if server down
5. how to recover the the how system if the rabbitmq-server crash  
6. add mysql model
'''
class Server:
	def __init__(self, serverName='localhost', port = None, userName='guest', passwdName='guest',
		               vhostName='radio_guest', exchangeName='direct_logs',
		               db_hostName='localhost', db_userName='test',db_passwdname='',db_dbName='test'):
		self.cmd_msg_props = Utility.GetCmdMsgProps()
		self.thread_msg_props = Utility.GetThreadMsgProps()
		self.port = port
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
									 port=self.port,
									 userName=self.userName,
									 passwdName=self.passwdName,
									 vhostName=self.vhostName)
