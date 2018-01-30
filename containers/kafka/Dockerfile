FROM centos:centos6

RUN yum -y update
RUN yum -y install wget
RUN yum -y clean all

RUN yum install java-1.7.0-openjdk -y
RUN yum install java-1.7.0-openjdk-devel -y
RUN yum update -y

RUN wget http://mirrors.dotsrc.org/apache/kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz


RUN tar xvzf kafka_2.11-0.10.2.1.tgz
RUN mv kafka_2.11-0.10.2.1 kafka
