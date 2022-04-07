FROM node:16-alpine3.11 AS frontend

COPY ./frontend ./app/

RUN cd /app && npm install && export NODE_ENV=production && npm run build

FROM adoptopenjdk:11 AS backend

# copy the pom and src code to the container
COPY ./ ./app

# package our application code, we optimize with extra JVM flags and disable tests because Maven throws java.lang.OutOfMemoryError: GC overhead limit exceeded
RUN cd app && export MAVEN_OPTS="-Xmx2500m -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:-UseGCOverheadLimit" && ./mvnw package -Dmaven.test.skip=true

# the second stage of our build will use open jdk 11 on alpine 3.9
FROM openjdk:11-jre-slim

# copy only the artifacts we need from the first stage and discard the rest
COPY --from=backend /app/target/metriql-*-bundle /

# see https://cloud.google.com/sdk/docs/install#deb
RUN apt-get update && apt-get install apt-transport-https ca-certificates gnupg curl -y
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# install packages required at runtime
RUN apt-get update && apt-get install python-dev python3-pip google-cloud-sdk -y && pip3 install "pip>=20" && pip3 install metriql-lookml==0.3 metriql-tableau==0.6 metriql-superset==0.6 metriql-metabase==0.5

RUN mv metriql-* app

COPY --from=frontend /app/dist /app/frontend

# set the startup command to execute the jar
ENTRYPOINT ["java", "-cp", "/app/lib/*", "com.metriql.ServiceStarterKt"]
EXPOSE 5656