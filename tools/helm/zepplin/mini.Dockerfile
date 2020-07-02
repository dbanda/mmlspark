FROM sparktest:latest
MAINTAINER Dalitso Banda <dalitsohb@gmail.com>

ADD patch_beam.patch /tmp/patch_beam.patch

ENV Z_VERSION="0.9.0-preview1"
# ENV Z_COMMIT="2ea945f548a4e41312026d5ee1070714c155a11e"
ENV LOG_TAG="[ZEPPELIN_${Z_VERSION}]:" \
    Z_HOME="/zeppelin" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG setting python dependencies" && \
    apk add --no-cache python && \
    pip install --no-cache-dir --upgrade pip setuptools && \
    rm -rf /root/.cache && \

    echo "$LOG_TAG Install essentials" && \
    apk add --no-cache git wget curl && \
    apk add --no-cache && \
    apk --no-cache --update add ca-certificates && \
    apk --no-cache --update add libstdc++ libstdc++6 && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.26-r0/glibc-2.26-r0.apk && \
    apk del --no-cache --update libc6-compat && \
    apk add --no-cache --update glibc-2.26-r0.apk
    # echo "$LOG_TAG install nodejs" && \
    # apk add --no-cache --update nodejs nodejs-npm

RUN echo "$LOG_TAG Download Zeppelin binary" && \
    wget -O /tmp/zeppelin-${Z_VERSION}-bin-all.tgz http://archive.apache.org/dist/zeppelin/zeppelin-${Z_VERSION}/zeppelin-${Z_VERSION}-bin-all.tgz && \
    tar -zxvf /tmp/zeppelin-${Z_VERSION}-bin-all.tgz && \
    rm -rf /tmp/zeppelin-${Z_VERSION}-bin-all.tgz && \
    mkdir -p ${Z_HOME} && \
    mv /zeppelin-${Z_VERSION}-bin-all/* ${Z_HOME}/ && \
    chown -R root:root ${Z_HOME} && \
    mkdir -p ${Z_HOME}/logs ${Z_HOME}/run ${Z_HOME}/webapps && \
    # Allow process to edit /etc/passwd, to create a user entry for zeppelin
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    # Give access to some specific folders
    chmod -R 775 "${Z_HOME}/logs" "${Z_HOME}/run" "${Z_HOME}/notebook" "${Z_HOME}/conf" && \
    # Allow process to create new folders (e.g. webapps)
    chmod 775 ${Z_HOME} && \
    echo "deleting rarely used intepretors" && \
    rm -rf ${Z_HOME}/interpreter/pig && \
    rm -rf ${Z_HOME}/interpreter/flink && \
    rm -rf ${Z_HOME}/interpreter/scio && \
    rm -rf ${Z_HOME}/interpreter/beam && \
    rm -rf ${Z_HOME}/interpreter/scalding && \
    rm -rf ${Z_HOME}/interpreter/geode && \
    rm -rf ${Z_HOME}/interpreter/ignite && \
    rm -rf ${Z_HOME}/interpreter/alluxio && \
    rm -rf ${Z_HOME}/interpreter/hazelcastjet && \
    rm -rf ${Z_HOME}/interpreter/jdbc && \
    rm -rf ${Z_HOME}/interpreter/bigquery && \
    rm -rf ${Z_HOME}/interpreter/kylin && \
    rm -rf ${Z_HOME}/interpreter/sap && \
    rm -rf ${Z_HOME}/interpreter/cassandra && \
    rm -rf ${Z_HOME}/interpreter/groovy && \
    rm -rf ${Z_HOME}/interpreter/lens && \
    rm -rf ${Z_HOME}/interpreter/neo4j && \
    rm -rf ${Z_HOME}/interpreter/livy && \
    rm -rf ${Z_HOME}/interpreter/angular && \
    rm -rf ${Z_HOME}/interpreter/hbase

RUN echo "$LOG_TAG installing python related packages" && \
    apk add --no-cache g++ python-dev python3-dev build-base wget freetype-dev libpng-dev openblas-dev && \
    pip3 install -U pip && \
    pip3 install --no-cache-dir -U pip setuptools wheel && \
    # pip3 install --no-cache-dir numpy  matplotlib pandas && \
    rm /usr/bin/python && \
    rm /usr/bin/pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \ 
    ln -s /usr/bin/pip3 /usr/bin/pip && \
    echo "$LOG_TAG Cleanup" && \
    rm -rf /root/.npm && \
    rm -rf /root/.m2 && \
    rm -rf /root/.cache && \
    rm -rf /tmp/*

ADD jars /jars

# add notebooks
RUN mkdir ${Z_HOME}/notebook/mmlspark -p
ADD mmlsparkExamples/ ${Z_HOME}/notebook/mmlspark/

ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ADD zeppelin-env.sh ${Z_HOME}/conf/

# use python3 as default since thats what's in the base image \
RUN echo "export PYSPARK_DRIVER_PYTHON=python3" >> ${Z_HOME}/conf/zeppelin-env.sh && \
    echo "export PYSPARK_PYTHON=python3" >> ${Z_HOME}/conf/zeppelin-env.sh

EXPOSE 8080

RUN apk add --no-cache tini
ENTRYPOINT [ "/sbin/tini", "--" ]

WORKDIR ${Z_HOME}
CMD ["sh", "-c", "echo '\nspark.driver.host' $(hostname -i) >> /opt/spark/conf/spark-defaults.conf && bin/zeppelin.sh"]

