package ru.ibs.dtm.query.execution.core.dto.eddl;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;

/**
 * Запрос создания внешней таблицы загрузки
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CreateUploadExternalTableQuery extends EddlQuery {
    /**
     * Тип
     */
    private Type locationType;

    /**
     * путь
     */
    private String locationPath;

    /**
     * Формат
     */
    private Format format;

    /**
     * Avro схема в формате Json
     */
    private String tableSchema;
    /**
     * Размер чанка
     */
    private Integer messageLimit;

    public CreateUploadExternalTableQuery() {
        super(EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE);
    }

    public CreateUploadExternalTableQuery(String schemaName,
                                          String tableName,
                                          Type locationType,
                                          String locationPath,
                                          Format format,
                                          String tableSchema,
                                          Integer messageLimit) {
        super(EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE, schemaName, tableName);
        this.locationType = locationType;
        this.locationPath = locationPath;
        this.format = format;
        this.tableSchema = tableSchema;
        this.messageLimit = messageLimit;
    }
}
