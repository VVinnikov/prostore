FROM openjdk:8-jdk-slim
WORKDIR app
ARG DTM_JAR
COPY ${DTM_JAR} dtm-query-execution-core.jar
ENTRYPOINT ["java", "-Dspring.config.location=./config/core/application.yml,./config/adb/application.yml,./config/adg/application.yml,./config/adqm/application.yml", "-jar", "dtm-query-execution-core.jar"]
