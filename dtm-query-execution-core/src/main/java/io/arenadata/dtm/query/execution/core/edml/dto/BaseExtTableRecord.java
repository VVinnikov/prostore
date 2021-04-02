package io.arenadata.dtm.query.execution.core.edml.dto;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.vertx.core.json.JsonObject;
import lombok.Data;

@Data
public class BaseExtTableRecord {
    private Long id;
    private Long datamartId;
    private String tableName;
    private Type locationType;
    private String locationPath;
    private ExternalTableFormat format;
    private JsonObject tableSchema;
}
