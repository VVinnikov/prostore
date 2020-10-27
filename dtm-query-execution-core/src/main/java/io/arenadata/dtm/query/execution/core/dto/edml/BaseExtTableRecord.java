package io.arenadata.dtm.query.execution.core.dto.edml;

import io.arenadata.dtm.common.plugin.exload.Format;
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
    private Format format;
    private JsonObject tableSchema;
}