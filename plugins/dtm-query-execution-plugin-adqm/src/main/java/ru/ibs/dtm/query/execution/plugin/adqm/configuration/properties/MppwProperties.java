package ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mppw")
public class MppwProperties {
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public void setKafkaBrokers(String kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
    }

    private String consumerGroup;
    private String kafkaBrokers;
}
