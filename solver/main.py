from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import Producer
import json
import numpy as np
import math
import time

def solution(json_str):
    for i in range(1,2):
        nome = '/instances.json'
        arquivo = open(nome, 'r') 
        conteudo = arquivo.read()
        json_importado=conteudo
        vimported = json.loads(json_importado)
        m = vimported['m'] 
        price = vimported['price']
        json_importado=json_str
        vimported = json.loads(json_importado)
        o = vimported['o'] 
        ow = vimported['ow'] 
        s = vimported['s'] 
        sw = vimported['sw'] 
        maxprice = vimported['maxprice']
        qtdepedidoimp = vimported['count']
        idx = vimported['idx']
        return_data = []
        
        #Parameters
        M = len(m)
        N = len(o)+len(s)
        P = len(price)
        passou = []
        qtdepedido = qtdepedidoimp[0]
        qtdepassou = 0
        
        # Verificação para saber quais máquinas atendem aos requisitos do usuário (recursos objetivos e preço) e informar se haverá máquinas suficientes
        for i in range(0,M):
            if m[i][0]>=o[0] and m[i][1]>=o[1] and m[i][2]>=o[2] and m[i][3]<=o[3] and price[i]<=maxprice[0]:
                passou.append(1)
            else:
                passou.append(0)
                
        if qtdepedido > qtdepassou:
            return_data.append("Máquinas insuficientes")

        # Restrições
        if(ow[0]+ow[1]+ow[2]+ow[3]+sw[0]+sw[1]+sw[2]+sw[3]!=1):
            return_data.append("########################################")
            return_data.append("########################################")
            return_data.append("           ATENÇÃO:Pesos errados       ")
            return_data.append("########################################")
            return_data.append("########################################")

        #Valores normalizados
        normal_cpu = 0
        normal_mem = 0
        normal_storage = 0
        normal_latencia = 0
        normal_availability = 0
        normal_autonomy = 0 
        normal_reliability = 0
        normal_mobility = 0
        
        #Vetores Normalizados
        vetor_normal_cpu = []
        vetor_normal_mem = []
        vetor_normal_storage = []
        vetor_normal_latencia = []
        vetor_normal_availability = []
        vetor_normal_autonomy = []
        vetor_normal_reliability = []
        vetor_normal_mobility = []
        vetor_interessev1 = []
        vetor_interessev2 = []
        vetor_interessev2racional = []
        vetor_total_otimo = []
        vetor_total_racional = []

        #Calculo da Normalizacao
        for i in range(0,M):
            normal_cpu = normal_cpu + (m[i][0]**2)
            normal_mem = normal_mem + (m[i][1]**2)
            normal_storage = normal_storage + (m[i][2]**2)
            normal_latencia = normal_latencia + (m[i][3]**2)
            normal_availability = normal_availability + (m[i][4]**2)
            normal_autonomy = normal_autonomy + (m[i][5]**2) 
            normal_reliability = normal_reliability + (m[i][6]**2)
            normal_mobility = normal_mobility + (m[i][7]**2)

        normal_cpu = normal_cpu ** (1/2)
        normal_mem = normal_mem ** (1/2)
        normal_storage = normal_storage ** (1/2)
        normal_latencia = normal_latencia ** (1/2)
        normal_availability = normal_availability ** (1/2)
        normal_autonomy = normal_autonomy ** (1/2)
        normal_reliability = normal_reliability ** (1/2)
        normal_mobility = normal_mobility ** (1/2)   
        
        #Geração de Vetores Normalizados
        for i in range(0,M):
            cpu = (m[i][0]-o[0])/normal_cpu*ow[0]
            vetor_normal_cpu.append(cpu)
            mem = (m[i][1]-o[1])/normal_mem*ow[1]
            vetor_normal_mem.append(mem)
            storage = (m[i][2]-o[2])/normal_storage*ow[2]
            vetor_normal_storage.append(storage)
            latencia = -1*((m[i][3]-o[3])/normal_latencia*ow[3])
            vetor_normal_latencia.append(latencia)
            availability = (m[i][4]-s[0])/normal_availability*sw[0]
            vetor_normal_availability.append(availability)
            autonomy = (m[i][5]-s[1])/normal_autonomy*sw[1]
            vetor_normal_autonomy.append(autonomy)
            reliability = (m[i][6]-s[2])/normal_reliability*sw[2]
            vetor_normal_reliability.append(reliability)
            mobility = (m[i][7]-s[3])/normal_mobility*sw[3]
            vetor_normal_mobility.append(mobility)
            vetor_interessev1.append(vetor_normal_cpu[i] + vetor_normal_mem[i] + vetor_normal_storage[i] + vetor_normal_latencia[i])
            vetor_interessev2.append(vetor_normal_availability[i] + vetor_normal_autonomy[i] + vetor_normal_reliability[i] + vetor_normal_mobility[i])
            vetor_interessev2racional.append(abs(vetor_normal_availability[i]) + abs(vetor_normal_autonomy[i]) + abs(vetor_normal_reliability[i]) + abs(vetor_normal_mobility[i]))
            vetor_total_otimo.append(vetor_interessev1[i] + vetor_interessev2[i])
            vetor_total_racional.append(vetor_interessev1[i] + vetor_interessev2racional[i])
        
        #Calculo do melhor custo beneficio com melhor máquina (usuário)
        livre = []
        for i in range(0,M):
            livre.append(0)

        for q in range(0,qtdepedido):
            custobeneficio = []
            cb = 0
            melhorcb = 999999
            for i in range(0,M):
                if passou[i] == 1:
                    cb = price[i]/vetor_total_otimo[i]
                    custobeneficio.append(cb)
                    if cb < melhorcb and livre[i] == 0:
                        melhorcb = cb
                else:
                    custobeneficio.append(999999)

            for i in range(0,M):
                if custobeneficio[i] == melhorcb and custobeneficio[i] != 999999:
                    return_data.append("Maquina %d melhor custo beneficio (USER) = %.2f"%(i+1, custobeneficio[i]))
                    livre[i] = 1
        
        #Calculo do melhor custo beneficio com máquina mais justa (provedor)
        livre = []
        for i in range(0,M):
            livre.append(0)

        for q in range(0,qtdepedido):
            custobeneficio = []
            cb = 0
            melhorcbprov = 0
            for i in range(0,M):
                if passou[i] == 1:
                    cb = price[i]/vetor_total_racional[i]
                    custobeneficio.append(cb)
                    if cb > melhorcbprov and livre[i] == 0:
                        melhorcbprov = cb
                else:
                    custobeneficio.append(999999)
            for i in range(0,M):
                if custobeneficio[i] == melhorcbprov and custobeneficio[i] != 0:
                    return_data.append("Maquina %d melhor custo beneficio (PROVEDOR) = %.2f"%(i+1, custobeneficio[i]))
                    livre[i] = 1

    content = "\n".join(return_data)
    print(content)
    content_json = json.dumps({"content":content, "idx":idx})
    print(content_json)
    createTopic(content_json, str(idx))

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createConsumer():
    topics = ['allocation']
    conf = {        
        'bootstrap.servers': 'kafka:9092',
        'group.id': "%s-consumer" % 'allocation',
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
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
                solution(msg.value().decode('utf-8'))
    except KeyboardInterrupt:        
        sys.stderr.write('%% Aborted by user\n')    
        c.close()

def createTopic(data, topic):
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

createConsumer()
