package io.arenadata.dtm.query.execution.core.eddl.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.plugin.exload.Type;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

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
    private ExternalTableFormat format;

    /**
     * Avro schema in json format
     */
    private String tableSchema;
    /**
     * Chunk size
     */
    private Integer messageLimit;

    @Builder
    public CreateUploadExternalTableQuery(String schemaName,
                                          String tableName,
                                          Entity entity,
                                          Type locationType,
                                          String locationPath,
                                          ExternalTableFormat format,
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
