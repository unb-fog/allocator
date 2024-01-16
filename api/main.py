from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createTopic(data):    
    topic = 'allocation'
    bootstrapServers = 'kafka:9092'
    #username = 'nome-do-usuario'
    #password = 'senha-do-usuario'
    conf = {
        'bootstrap.servers': bootstrapServers,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        #'security.protocol': 'SASL_SSL',
	    #'sasl.mechanisms': 'SCRAM-SHA-256',
        #'sasl.username': username,
        #'sasl.password': password
    }

    p = Producer(conf)

    try:
        p.produce(topic, data, callback=delivery_callback)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(p))    
        p.poll(0)

    print('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

def createConsumer(topic):
    topics = [ topic ]
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': "%s-consumer" % 'allocation',
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
        #'security.protocol': 'SASL_SSL',
        #'sasl.mechanisms': 'SCRAM-SHA-256',
        #'sasl.username': 'username',
        #'sasl.password': 'password'
        }
    try:
        c = Consumer(conf)
        c.subscribe(topics)
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print( msg.error )
            else:
                print(round(time.time() * 1000))
                print("------------------->")
                print(msg.value)
                idx = json.loads(msg.value.decode('utf-8'))["idx"]
                content = json.loads(msg.value.decode('utf-8'))["content"]
                requests[idx] = content
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
        c.close()

class Parameters(BaseModel):
    o: list
    ow: list
    s: list
    sw: list
    maxprice: list
    count: list

app = FastAPI()

requests = {}

@app.get("/")
def read_root():
    return {"It works"}

@app.post("/allocation/")
async def create_item(request: Parameters):
    idx = str(round(time.time() * 1000))
    content_dict = request.__dict__
    content_dict["idx"] = idx
    content = json.dumps(content_dict)
    createTopic(content)
    requests[idx] = False
    createConsumer(idx)
    while requests[idx] == False:
      print("waiting request "+idx)
    return requests[idx]
