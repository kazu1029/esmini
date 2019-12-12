FROM docker.elastic.co/elasticsearch/elasticsearch:7.4.2

ENV ES_JAVA_OPTS="-Xms750m -Xmx750m"
ENV discovery.type="single-node"
ENV http:host="0.0.0.0"
ENV transport.host="127.0.0.1"
ENV xpack.security.enabled=false

RUN bin/elasticsearch-plugin install analysis-kuromoji
RUN bin/elasticsearch-plugin install analysis-icu
