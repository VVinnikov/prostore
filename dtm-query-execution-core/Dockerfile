FROM openjdk:8-jdk-slim
WORKDIR app
RUN mkdir config
COPY target/classes/config/ config/
ADD target/dtm-query-execution-core-*.jar dtm-query-execution-core.jar
EXPOSE 8080
ENTRYPOINT ["java", "-Dspring.config.location=classpath:application.yml,config/application.yml,classpath:config/plugin/adb/application.yml,classpath:config/plugin/adg/application.yml,classpath:config/plugin/adqm/application.yml", "-jar", "dtm-query-execution-core.jar"]
