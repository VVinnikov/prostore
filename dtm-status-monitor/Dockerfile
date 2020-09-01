FROM openjdk:8-jdk-slim
WORKDIR app
RUN mkdir config
ADD /target/dtm-status-monitor-*.jar dtm-status-monitor.jar
ENV STATUS_MONITOR_BROKERS=localhost:9092
ENV STATUS_MONITOR_CONSUMERS=8
ENV SERVER_PORT=9095
EXPOSE 9095
ENTRYPOINT ["java", "-Dspring.config.location=classpath:application.yml,config/application.yml", "-jar", "dtm-status-monitor.jar"]
