package io.arenadata.dtm.common.cache;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryTemplateValue {
    private String enrichQueryTemplate;
}
