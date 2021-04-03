package io.arenadata.dtm.query.execution.core.edml.mppw.dto;

import io.arenadata.dtm.query.execution.core.edml.dto.BaseExtTableRecord;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExtTableRecord extends BaseExtTableRecord {
    private JsonObject tableSchema;
    private Integer messageLimit;
}
