from typing import Union
from fastapi import FastAPI
from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createTopic(q):    
    topic = 'nome-do-topico'
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
        data = q
        p.produce(topic, data, callback=delivery_callback)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(p))    
        p.poll(0)

    print('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    createTopic(q)
    return {"item_id": item_id, "q": q}