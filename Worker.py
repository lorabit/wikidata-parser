
# coding: utf-8

# In[24]:





# In[13]:


import bz2
import json
import ast
import traceback
from pymongo import MongoClient
import pickle
def consume(thread_id, msg):
    def check_buffer(force_write = False):
        if (force_write and len(entity_buffer)>0) or len(entity_buffer)>=buffer_size:
            print("%s - Writing %d entities into database ..."%(thread_id,len(entity_buffer)))
            db.entities.insert_many(entity_buffer)
            entity_buffer.clear()
    #         print("%d entities writen."%(len(ids)))

        if (force_write and len(triplet_buffer)>0) or len(triplet_buffer)>=buffer_size:
            print("%s - Writing %d triplets into database ..." % (thread_id, len(triplet_buffer)))
            db.triplets.insert_many(triplet_buffer)
            triplet_buffer.clear()
    #         print("%d triplets writen."%(len(ids)))

    def generate_triplet(tripletId, sub, pre, obj):
        triplet_buffer.append({
            "wikiDataId": tripletId,
            "entityId": sub,
            "predicate": pre,
            "value": obj
        })

    def generate_entity(entityId, label, aliases):
        entity_buffer.append({
            "wikiDataId": entityId,
            "label": label,
            "aliases": aliases
        })

    def has_valid_qualifiers(d):
        if d.get("qualifiers") == None:
            return False
        for i in d["qualifiers"]:
            if d["qualifiers"][i][0]["datatype"] in valid_datatypes and d["qualifiers"][i][0]["snaktype"]=="value":
                return True
        return False

    def extract_triplet(sub, d):
        if not d[0]["mainsnak"]["datatype"] in valid_datatypes:
            return
        for i in d:
            if i["mainsnak"]["snaktype"]!="value":
                continue
            if has_valid_qualifiers(i):
                # Generate CVT
                cvt = i["id"]
                generate_triplet(i["id"], sub, i["mainsnak"]["property"]+"A", {"value":cvt, "type":"cvt"})
                generate_triplet(i["id"], cvt, i["mainsnak"]["property"]+"B", i["mainsnak"]["datavalue"])
                for j in i["qualifiers"]:
                    k = i["qualifiers"][j]
                    for q in k:
                        if q.get("datatype")!=None and q["datatype"] in valid_datatypes and q["snaktype"]=="value":
                            generate_triplet(i["id"], cvt, q["property"], q["datavalue"])
                return
    #         print(i["id"])
    #         print(i["mainsnak"])
            generate_triplet(i["id"], sub, i["mainsnak"]["property"], i["mainsnak"]["datavalue"])

    def extract_document(d):
        if d["labels"].get("en")==None:
            return
        names = [d["labels"]["en"]["value"]]
        if d["aliases"].get("en")!=None:
            for i in d["aliases"]["en"]:
                names.append(i['value'])
        generate_entity(d["id"], names[0], names)
        for i in d["claims"]:
            extract_triplet(d["id"], d["claims"][i])

    def parse_document(s):
        s = str(s)
        j = ast.literal_eval(s[1:])[:-2]
        return json.loads(j)
    
    client = MongoClient('10.0.74', 27017)
    db = client.wikidata
    valid_datatypes = ["monolingualtext","string","wikibase-item","quantity","time"]
    buffer_size = 5000
    entity_buffer = []
    triplet_buffer = []
    for l in pickle.loads(msg):
        try:
            extract_document(parse_document(l))
            check_buffer()
        except Exception as err:
            print(l)
            traceback.print_tb(err.__traceback__)
    check_buffer(force_write = True)
    client.close()


# In[14]:


import pika
def thread_worker(thread_id):
    credentials = pika.PlainCredentials('wikidata', 'wikidata')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.74',5672,'/',credentials))
    channel = connection.channel()
    channel.queue_declare(queue='wikidata', durable=True)
    def callback(ch, method, properties, body):
        consume(thread_id, body)
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='wikidata')

    print("%s start consuming." % thread_id)
    channel.start_consuming()


# In[15]:


import threading
class workerThread (threading.Thread):
   def __init__(self, thread_id):
      threading.Thread.__init__(self)
      self.thread_id = thread_id
   def run(self):
      thread_worker(self.thread_id)

threads = {}
for thread_id in ["t1","t2","t3","t4"]:
    threads[thread_id] = workerThread(thread_id)
    threads[thread_id].start()

for thread_id in threads:
    threads[thread_id].join()

