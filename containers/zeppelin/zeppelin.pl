#!/usr/bin/perl

use strict;

my $filename = 'Dockerfile';
open(my $fh, '>', 'Dockerfile');

print $fh "FROM ubuntu:14.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN  apt-get update
RUN  apt-get install -y wget";

print $fh "
RUN  apt-get install openjdk-7-jre -y
RUN apt-get install openjdk-7-jdk -y
RUN apt-get install python-software-properties -y
RUN apt-get update -y";


print $fh "
WORKDIR \/root

RUN wget http:\/\/apache.mirrors.pair.com\/zeppelin\/zeppelin-0.7.2\/zeppelin-0.7.2-bin-all.tgz
RUN tar zxvf zeppelin-0.7.2-bin-all.tgz -C \/opt
RUN  chown -R root:root \/opt\/zeppelin-0.7.2-bin-all\/
RUN  ln -fs \/opt\/zeppelin-0.7.2-bin-all \/opt\/zeppelin";

print $fh "

\RUN sed -i '\$ a\\PATH=\$PATH:/opt/zeppelin/bin' .bashrc\
\RUN sed -i '\$ a\\export PATH' .bashrc\

RUN source  ~\/.bashrc";

print $fh "
WORKDIR \/opt\/zeppelin\/conf
RUN cp zeppelin-site.xml.template zeppelin-site.xml

EXPOSE 8080";

close $fh;

system("docker build -t zeppelin . ");
