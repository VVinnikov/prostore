package io.arenadata.dtm.query.execution.core.dto.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EntityKey {
    private final String datamartName;
    private final String entityName;
}
