FROM openjdk:8-jdk-slim
WORKDIR app
ARG DTM_JAR
COPY ${DTM_JAR} dtm-query-execution-core.jar
ENTRYPOINT ["java", "-jar", "./dtm-query-execution-core.jar"]
