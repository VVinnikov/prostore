package io.arenadata.dtm.query.execution.core.eddl.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Запрос удаления внешней загрузки
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DropUploadExternalTableQuery extends EddlQuery {

    public DropUploadExternalTableQuery() {
        super(EddlAction.DROP_UPLOAD_EXTERNAL_TABLE);
    }

    public DropUploadExternalTableQuery(String schemaName, String tableName) {
        super(EddlAction.DROP_UPLOAD_EXTERNAL_TABLE, schemaName, tableName);
    }
}
