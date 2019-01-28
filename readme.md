Github Events Kafka Producer/Consumer
=====================================

This is a sample development project that constain following modules
1. GitHub Event Kafka Producer (github-event-producer) - This module consume GitHub public events api (https://api.github.com/events) and write it into Kafka running on localhost after removing "payload" key from each event
2. GitHub Event Kafka Consumer (github-event-consumer) - This module consumer GitHub public events written into Kafka by Kafka producer and send it to ElasticSearch cluster running on localhost 

Pre-requisite
-------------
- Kafka 2.0.0 running on localhost
- ElasticSearch 6.5 running on localhost

Notes
-----
Not for production use directly however you can refer to the code and make it production ready. I did it just for learning :)


