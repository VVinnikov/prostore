package ru.ibs.dtm.query.execution.core.dto.eddl;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

/**
 * Upload External table creation request
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CreateUploadExternalTableQuery extends EddlQuery {
    /**
     * Table entity
     */
    private Entity entity;

    /**
     * Type
     */
    private Type locationType;

    /**
     * Path
     */
    private String locationPath;

    /**
     * Format
     */
    private Format format;

    /**
     * Avro schema in json format
     */
    private String tableSchema;
    /**
     * Chunk size
     */
    private Integer messageLimit;

    public CreateUploadExternalTableQuery() {
        super(EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE);
    }

    public CreateUploadExternalTableQuery(String schemaName,
                                          String tableName,
                                          Entity entity,
                                          Type locationType,
                                          String locationPath,
                                          Format format,
                                          String tableSchema,
                                          Integer messageLimit) {
        super(EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE, schemaName, tableName);
        this.entity = entity;
        this.locationType = locationType;
        this.locationPath = locationPath;
        this.format = format;
        this.tableSchema = tableSchema;
        this.messageLimit = messageLimit;
    }
}
