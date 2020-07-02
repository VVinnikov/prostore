package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.reader.QueryRequest;

public class StatusRequest extends DatamartRequest{

    private String topic;

    public StatusRequest(QueryRequest queryRequest) {
        super(queryRequest);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}
