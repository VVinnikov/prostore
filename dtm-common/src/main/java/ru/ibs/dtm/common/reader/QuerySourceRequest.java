package ru.ibs.dtm.common.reader;

import lombok.*;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

/*Дто с модифицированным sql запросом, из которого извлечен хинт*/
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    @NonNull
    private SourceType sourceType;
}
