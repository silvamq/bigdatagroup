FROM centos

# RUN yum -y update
RUN yum -y install java-1.8.0-openjdk
RUN yum -y install wget
RUN yum -y install nc
RUN wget http://ftp.unicamp.br/pub/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar -xvf spark-2.4.0-bin-hadoop2.7.tgz
RUN rm spark-2.4.0-bin-hadoop2.7.tgz

ENV PATH "$PATH:/spark-2.4.0-bin-hadoop2.7/bin"
ENV PATH "$PATH:/spark-2.4.0-bin-hadoop2.7/sbin"

ADD ./sh /root
ENV HOME /root
WORKDIR /root

#CMD /root/master.sh
