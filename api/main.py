from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time
import logging

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createTopic(data, topic):
    print("Create topic "+topic)   
    bootstrapServers = 'kafka:9092'
    conf = {
        'bootstrap.servers': bootstrapServers,
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
        'group.id': "%s-consumer" % topic,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
    try:
        c = Consumer(conf)
        c.subscribe(topics)
        while True:
            msg = c.poll(timeout=1.0)
            logger.debug("Waiting response")
            if msg is None:
                continue
            if msg.error():
                print( msg.error )
            else:
                return msg.value().decode('utf-8')
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

logging.basicConfig(level=logging.DEBUG)   # add this line
logger = logging.getLogger("default")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, log_level="debug")

@app.get("/")
def read_root():
    return {"It works"}

@app.post("/allocation/")
async def create_item(request: Parameters):
    idx = str(round(time.time() * 1000))
    logger.debug("idx="+idx)
    content_dict = request.__dict__
    content_dict["idx"] = idx
    content = json.dumps(content_dict)
    createTopic(content, 'allocation')
    res = createConsumer(str(idx))
    result_json = json.loads(res) 
    return result_json["content"]