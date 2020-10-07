package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public class LlrRequest extends DatamartRequest {

    private List<Datamart> schema;
    private List<ColumnMetadata> metadata;

    public LlrRequest(QueryRequest queryRequest, List<Datamart> schema, List<ColumnMetadata> metadata) {
        super(queryRequest);
        this.schema = schema;
        this.metadata = metadata;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

    public void setSchema(List<Datamart> schema) {
        this.schema = schema;
    }

    public List<ColumnMetadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<ColumnMetadata> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "LlrRequest{" +
                "schema=" + schema +
                ", metadata=" + metadata +
                '}';
    }
}
