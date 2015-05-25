import pika
import sys


'''
fetch the specific queue's message
'''
class fetch_queue_message:
	def __init__(self, id, serverName, port=None,
			     userName='guest', passwdName='guest', 
			     vhostName='radio_guest', exchangeName='direct_logs'):
		creds = pika.PlainCredentials(userName, passwdName)
		params = pika.ConnectionParameters(serverName,
										   port=port,
										   virtual_host=vhostName,
										   credentials=creds)
		conn = pika.BlockingConnection(params)
		channel = conn.channel()
		
		queueName = 'fetch_' + str(id)

		'''
		exclusive
			- When set to true, your queue becomes private and can only be consumed by your app.
				This is useful when you need to limit a queue to only one consumer.
		auto-delete 
			- The queue is automatically deleted when the last consumer unsubscribes.
				If you need a temporary queue used only by one consumer,combine autodelete with exclusive.
				When the consumer disconnects, the queue will be removed.
		'''
		channel.queue_declare(queue=queueName)
		channel.queue_bind(exchange=exchangeName,
						   queue=queueName,
					   	   routing_key=str(id))

		channel.basic_consume(self.callback,
							  queue=queueName,
							  no_ack=True)

		self.channel = channel
		self.queueName = queueName

	def callback(self, ch, method, properties, body):
		print body

	def start_consume(self):
		self.channel.start_consuming()

	def delete_queue(self, queueName=""):
		self.channel.stop_consuming()
		if queueName:
			self.channel.queue_delete(queue=queueName)
		else:
			self.channel.queue_delete(queue=self.queueName)

if __name__ == "__main__":
	queue = fetch_queue_message(id=sys.argv[1], serverName=sys.argv[2])
	queue.start_consume()
