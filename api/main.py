from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
import json

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createTopic(data):    
    print(data)
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

class Parameters(BaseModel):
    o: list
    ow: list
    s: list
    sw: list
    maxprice: list
    count: list

app = FastAPI()

@app.get("/")
def read_root():
    return {"It works"}

@app.post("/allocation/")
async def create_item(request: Parameters):
    createTopic(json.dumps(request.__dict__))
    return request