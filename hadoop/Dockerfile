FROM mdouchement/hdfs:latest

EXPOSE 22022 22
EXPOSE 8020 8020
EXPOSE 50010 50010
EXPOSE 50020 50020
EXPOSE 50070 50070
EXPOSE 50075 50075

COPY sample.json /tmp/sample.json
COPY twitter-sample.json /tmp/twitter-sample.json

#RUN && hadoop fs -mkdir /user \
#	&& hadoop fs -mkdir /user/gdd \
#	&& hadoop fs -put /tmp/sample.json /tmp/twitter-sample.json /user/gdd

CMD service ssh start \
  && start-dfs.sh \
  && hadoop-daemon.sh start portmap \
  && hadoop-daemon.sh start nfs3 \
  && hadoop fs -mkdir /user \
  && hadoop fs -mkdir /user/gdd \
  && hadoop fs -put /tmp/sample.json /tmp/twitter-sample.json /user/gdd \
  && bash