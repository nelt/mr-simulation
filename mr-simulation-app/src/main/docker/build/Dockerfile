FROM openjdk:13-alpine3.10

RUN mkdir -p /usr/lib/mr-simulation
RUN mkdir -p /work

COPY mr-${project.version}.jar  /usr/lib/mr-simulation/mr.jar
COPY mr.sh /usr/bin/mr
RUN chmod a+x /usr/bin/mr

WORKDIR /work
CMD echo "Usage : mr --map map.js --reduce reduce.js --data-set data-set.json"

