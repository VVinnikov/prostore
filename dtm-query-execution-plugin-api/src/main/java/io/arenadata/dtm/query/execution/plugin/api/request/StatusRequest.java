package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;

public class StatusRequest extends DatamartRequest {

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
