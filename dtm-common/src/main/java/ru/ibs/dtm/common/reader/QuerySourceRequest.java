package ru.ibs.dtm.common.reader;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/*Дто с модифицированным sql запросом, из которого извлечен хинт*/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    @NonNull
    private SourceType sourceType;

    public QuerySourceRequest(@NonNull QueryRequest queryRequest, @NonNull SourceType sourceType) {
        this.queryRequest = queryRequest;
        this.sourceType = sourceType;
    }
}
