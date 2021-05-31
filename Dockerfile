FROM adoptopenjdk:8 AS MAVEN_BUILD

# copy the pom and src code to the container
COPY ./ ./app

# package our application code, we optimize with extra JVM flags and disable tests because Maven throws java.lang.OutOfMemoryError: GC overhead limit exceeded
RUN cd app && export MAVEN_OPTS="-Xmx2500m -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:-UseGCOverheadLimit -XX:PermSize=512m" && ./mvnw package -Dmaven.test.skip=true

# the second stage of our build will use open jdk 8 on alpine 3.9
FROM openjdk:8-jre-alpine3.9

# copy only the artifacts we need from the first stage and discard the rest
COPY --from=MAVEN_BUILD /app/target/metriql-*-bundle /

RUN mv metriql-* app

# set the startup command to execute the jar
ENTRYPOINT ["java", "-cp", "app/lib/*", "com.metriql.ServiceStarterKt"]