package ru.ibs.dtm.query.execution.plugin.api.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.common.reader.QueryRequest;

@Data
@AllArgsConstructor
public class DatamartRequest {
    private QueryRequest queryRequest;
}
