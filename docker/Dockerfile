FROM tjake/jepsen

RUN mkdir /jepsen_tidb
# Create the VOLUMN so that we can run our local host source.
VOLUME /jepsen_tidb

ADD ./bashrc /root/.bashrc