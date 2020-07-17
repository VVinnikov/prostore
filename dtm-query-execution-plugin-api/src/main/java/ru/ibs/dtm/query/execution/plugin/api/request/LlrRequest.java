package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public class LlrRequest extends DatamartRequest {

    private List<Datamart> schema;

    public LlrRequest(QueryRequest queryRequest, List<Datamart> schema) {
        super(queryRequest);
        this.schema = schema;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

    public void setSchema(List<Datamart> schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "LlrRequest{" +
                super.toString() +
                ", schema=" + schema +
                '}';
    }
}
