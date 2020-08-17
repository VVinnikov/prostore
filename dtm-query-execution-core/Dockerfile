FROM openjdk:8-jdk-slim
WORKDIR app
ARG DTM_JAR
COPY ${DTM_JAR} dtm-query-execution-core.jar
ENTRYPOINT ["java", "-agentlib:jdwp=transport=dt_socket,address=35286,server=y,suspend=n", "-jar", "./dtm-query-execution-core.jar"]
