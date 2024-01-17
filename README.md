# Fog Resource Allocator with Kafka

## Requisites
 - Docker Engine
 - Docker Compose
 - Git

## How to configure
 - Clone this project
 - In the project root create the .env file with `HOSTNAME={YOUR IP ADDRESS}`
 - run `docker compose up -d --build`
 - After is up and running wait about 20 seconds

## How to use
 - Using a REST Client make a POST request to `http://localhost:8080/allocation` with json formated parameters in the body and the Content-Type=application/json header.

Parameters Json example: `{"o": [4, 16, 2000, 4000], "ow": [0.125, 0.125, 0.125, 0.125], "s": [5, 2, 1, 3], "sw": [0.125, 0.125, 0.125, 0.125], "maxprice": [0.32], "count": [3]}`
