
# coding: utf-8

# In[3]:


import pika
import pickle
credentials = pika.PlainCredentials('wikidata', 'wikidata')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.74',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='wikidata', durable=True)
def produce_message(msg):
    channel.basic_publish(exchange='',
                      routing_key='wikidata',
                      body=pickle.dumps(msg),
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))


# In[ ]:





# In[5]:


import bz2
import json
import ast
import traceback
batch_size = 100
batch = []
with bz2.BZ2File("latest-all.json.bz2","r") as f:
    for l in f:
        if len(l)>10:
            batch.append(l)
            if len(batch) == batch_size:
                produce_message(batch)
                batch.clear()
produce_message(batch)

