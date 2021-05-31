FROM maven:3.6.1-jdk-8-alpine AS MAVEN_BUILD

# copy the pom and src code to the container
COPY ./ ./

# package our application code, we optimize with extra JVM flags and disable tests because Maven throws java.lang.OutOfMemoryError: GC overhead limit exceeded
RUN mvn dependency:resolve
RUN export MAVEN_OPTS="-Xmx1524M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit" && ./mvnw package -Dmaven.test.skip=true

# the second stage of our build will use open jdk 8 on alpine 3.9
FROM openjdk:8-jre-alpine3.9

# copy only the artifacts we need from the first stage and discard the rest
COPY --from=MAVEN_BUILD /metriql/target/metriql-*-bundle /

# set the startup command to execute the jar
ENTRYPOINT ["java", "-cp", "lib/*", "com.metriql.ServiceStarterKt"]