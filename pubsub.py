#!/usr/bin/env python


# ----------------------------------------------
# Google PubSub Demo
# Using Fake Stock Price 
# Publisher -> Subsriber
# This is meant to give a basic infrastructure 
# of understand the concept of PubSub
# ----------------------------------------------


import time
import argparse
import random 
import datetime 

from google.cloud import pubsub


topic_name = 'stock'
subscriber_name = 'stockReceiver'

def create_topic():	
	pubsub_client = pubsub.Client()
	topic = pubsub_client.topic(topic_name)
	topic.create()
	print 'Topic {} created'.format(topic_name)

def create_subscriber():
	pubsub_client = pubsub.Client()
	topic = pubsub_client.topic(topic_name)
	subscriber = topic.subscription(subscriber_name)
	subscriber.create()
	print 'Subscriber {} created'.format(subscriber_name)

def publish_message(data):
	pubsub_client = pubsub.Client()
	topic = pubsub_client.topic(topic_name)
	data = data.encode('utf-8')
	message_id = topic.publish(data)
	print 'Message ID:{} published to topic {}'.format(message_id,topic_name)

def receive_message(counter):
	pubsub_client = pubsub.Client()
	topic = pubsub_client.topic(topic_name)
	subscriber = topic.subscription(subscriber_name)
	for i in xrange(len(counter)):
		results = subscriber.pull(return_immediately=False)
		if results:
			for ack_id, message in results:
				print '* {}: {}, {}'.format(message.message_id, message.data, message.attributes)
				subscriber.acknowledge([ack_id for ack_id, message in results])


# ----------------------------------------------
# Fabricate a simple stock prrice using 
# random movements 
# ----------------------------------------------

def stock_price(price):
	if random.random() > .5: 
		price = price + random.random()
	else:
		price = price - random.random()
	return price

def today_timestamp():
	d = datetime.date.today()
	ymd = d.isoformat()
	return ymd

def deliver_stock_price(price):
	counter = 1 
	today = today_timestamp()
	hms_time = datetime.datetime.strptime('9:30:00', '%H:%M:%S')
	quote = 'GOOGL'
	name = 'Alphabet Inc.'
	stock = price
	for i in xrange(0,10):
		hms_time = hms_time + datetime.timedelta(0,1)
		stock = stock_price(stock)
		data = '{},{},{},{},{}'.format(today,hms_time.strftime('%H:%M:%S'),quote,name,stock)
		publish_message(data)
		counter = counter + 1 

	return counter

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--price', help='Give an initial Google stock price', required=True, type=float)
	args = parser.parse_args()

	create_topic()
	create_subscriber()
	counter = deliver_stock_price(args.price)
	receive_message(counter)


