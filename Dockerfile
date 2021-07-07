FROM adoptopenjdk:11 AS MAVEN_BUILD

# copy the pom and src code to the container
COPY ./ ./app

RUN cd app

# package our application code, we optimize with extra JVM flags and disable tests because Maven throws java.lang.OutOfMemoryError: GC overhead limit exceeded
RUN export MAVEN_OPTS="-Xmx2500m -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:-UseGCOverheadLimit -XX:PermSize=512m" && ./mvnw package -Dmaven.test.skip=true

# the second stage of our build will use open jdk 11 on alpine
FROM openjdk:11-jre-alpine

# copy only the artifacts we need from the first stage and discard the rest
COPY --from=MAVEN_BUILD /app/target/metriql-*-bundle /

RUN apt-get update && apt-get install python-dev python3-pip -y && pip3 install "pip>=20" && pip3 install metriql-lookml==0.2 metriql-tableau==0.3

RUN mv metriql-* app

# set the startup command to execute the jar
ENTRYPOINT ["java", "-cp", "app/lib/*", "com.metriql.ServiceStarterKt"]