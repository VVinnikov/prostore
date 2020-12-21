package io.arenadata.dtm.query.execution.core.dto.eddl;

/**
 * Drop external table query
 */
public class DropDownloadExternalTableQuery extends EddlQuery {

    public DropDownloadExternalTableQuery(String schemaName, String tableName) {
        super(EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE, schemaName, tableName);
    }
}
