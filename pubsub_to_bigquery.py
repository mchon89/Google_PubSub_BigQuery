#!/usr/bin/env python


# ----------------------------------------------
# Google PubSub to BigQuery Demo
# Using Fake Stock Price 
# ----------------------------------------------


import time
import argparse
import random 
import datetime 
import json
from pprint import pprint

from google.cloud import pubsub
from google.cloud import bigquery


topic_name = 'stock'
subscriber_name = 'stockReceiver'
dataset = 'demo_stock'
table = 'google'

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
	today = today_timestamp()
	hms_time = datetime.datetime.strptime('9:30:00', '%H:%M:%S')
	quote = 'GOOGL'
	name = 'Alphabet Inc.'
	stock = price
	for i in xrange(0,100):
		hms_time = hms_time + datetime.timedelta(0,1)
		stock = stock_price(stock)
		data = '{},{},{},{},{}'.format(today,hms_time.strftime('%H:%M:%S'),quote,name,stock)
		publish_message(data)
		counter = counter + 1 
	return counter

def stream_data_bigquery(counter):
	pubsub_client = pubsub.Client()
	topic = pubsub_client.topic(topic_name)
	subscriber = topic.subscription(subscriber_name)

	bigquery_client = bigquery.Client()
	dataset = bigquery_client.dataset(dataset_name)
	table = dataset.table(table_name)

	batch = []
	i = 0
	while i < counter:
		results = subscriber.pull(return_immediately=False)
		if results:
			for ack_id, message in results:
				print '* {}: {}, {}'.format(message.message_id, message.data, message.attributes)
				subscriber.acknowledge([ack_id for ack_id, message in results])
				batch.append(message.data)
				if len(batch) < 5:
					i = i + 1
				else:
					for j in xrange(0,len(batch)):
						dict_ = {}
						data = batch[j].split(',')
						table.reload()
						errors = table.insert_data([data])
						if not errors:
							print('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
						else:
							print('Errors:')
							pprint(errors)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('--price', help='Give an initial Google stock price', required=True, type=float)
	args = parser.parse_args()

	create_topic()
	create_subscriber()
	counter = deliver_stock_price(args.price)
	stream_data_bigquery(counter)


