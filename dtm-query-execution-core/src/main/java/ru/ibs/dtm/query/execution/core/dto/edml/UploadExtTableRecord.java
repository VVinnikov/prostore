package ru.ibs.dtm.query.execution.core.dto.edml;

import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExtTableRecord extends BaseExtTableRecord {
    private JsonObject tableSchema;
    private Integer messageLimit;
}
