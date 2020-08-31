package ru.ibs.dtm.query.execution.core.dto.edml;

import io.vertx.core.json.JsonObject;
import lombok.Data;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

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
