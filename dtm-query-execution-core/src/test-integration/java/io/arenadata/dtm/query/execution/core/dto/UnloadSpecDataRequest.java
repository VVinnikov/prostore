package io.arenadata.dtm.query.execution.core.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class UnloadSpecDataRequest extends UnloadSchemaRequest {
    private List<List<Object>> data;
}
