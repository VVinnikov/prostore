package io.arenadata.dtm.common.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DatamartRequest {
    private QueryRequest queryRequest;
}
