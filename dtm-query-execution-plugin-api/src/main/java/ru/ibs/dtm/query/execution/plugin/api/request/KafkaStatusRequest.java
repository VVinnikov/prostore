package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.reader.QueryRequest;

public class KafkaStatusRequest extends DatamartRequest{
    private String consumerGroup;
    private String topic;
    private String partition;

    public KafkaStatusRequest(QueryRequest queryRequest) {
        super(queryRequest);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }
}
