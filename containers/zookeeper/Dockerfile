FROM debian:jessie

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get update && apt-get install -y openjdk-7-jre-headless wget

RUN wget -q -O - http://mirror.csclub.uwaterloo.ca/apache/zookeeper/zookeeper-3.3.6/zookeeper-3.3.6.tar.gz | tar -xzf - -C /opt \
        && mv /opt/zookeeper-3.3.6 /opt/zookeeper \
        && cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

EXPOSE 2181 2888 3888

WORKDIR /opt/zookeeper

#VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]
