package io.arenadata.dtm.query.execution.core.base.exception.materializedview;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.Set;

public class MaterializedViewValidationException extends DtmException {
    private static final String MULTIPLE_DATAMARTS_PATTERN = "Materialized view %s has multiple datamarts: %s in query: %s";
    private static final String DIFFERENT_DATAMARTS_PATTERN = "Materialized view %s has datamart [%s] not equal to query [%s]";
    private static final String CONFLICT_COLUMN_COUNT_PATTERN = "Materialized view %s has conflict with query columns wrong count, view: %d query: %d";
    private static final String CONFLICT_COLUMN_TYPES_PATTERN = "Materialized view %s has conflict with query types not equal for: %s view: %s, query: %s";
    private static final String CONFLICT_COLUMN_ACCURACY_PATTERN = "Materialized view %s has conflict with query columns type accuracy not equal for %s view: %d query: %s";
    private static final String CONFLICT_COLUMN_SIZE_PATTERN = "Materialized view %s has conflict with query columns type size not equal for %s view: %d query: %s";
    private static final String QUERY_DATASOURCE_TYPE = "Materialized view %s query DATASOURCE_TYPE not specified or invalid";
    private static final String VIEW_DATASOURCE_TYPE = "Materialized view %s DATASOURCE_TYPE has non exist items: %s";

    private MaterializedViewValidationException(String message) {
        super(message);
    }

    public static MaterializedViewValidationException multipleDatamarts(String name, List<Datamart> datamarts, String query) {
        return new MaterializedViewValidationException(String.format(MULTIPLE_DATAMARTS_PATTERN, name, datamarts, query));
    }

    public static MaterializedViewValidationException differentDatamarts(String name, String entityDatamart, String queryDatamart) {
        return new MaterializedViewValidationException(String.format(DIFFERENT_DATAMARTS_PATTERN, name, entityDatamart, queryDatamart));
    }

    public static MaterializedViewValidationException columnCountConflict(String name, int viewColumns, int queryColumns) {
        return new MaterializedViewValidationException(String.format(CONFLICT_COLUMN_COUNT_PATTERN, name, viewColumns, queryColumns));
    }

    public static MaterializedViewValidationException columnTypesConflict(String name, String columnName, ColumnType viewColumnType, ColumnType queryColumnType) {
        return new MaterializedViewValidationException(String.format(CONFLICT_COLUMN_TYPES_PATTERN, name, columnName, viewColumnType, queryColumnType));
    }

    public static MaterializedViewValidationException columnTypeAccuracyConflict(String name, String columnName, Integer viewAccuracy, Integer queryAccuracy) {
        return new MaterializedViewValidationException(String.format(CONFLICT_COLUMN_ACCURACY_PATTERN, name, columnName, viewAccuracy, queryAccuracy));
    }

    public static MaterializedViewValidationException columnTypeSizeConflict(String name, String columnName, Integer viewSize, Integer querySize) {
        return new MaterializedViewValidationException(String.format(CONFLICT_COLUMN_SIZE_PATTERN, name, columnName, viewSize, querySize));
    }

    public static MaterializedViewValidationException queryDataSourceInvalid(String name) {
        return new MaterializedViewValidationException(String.format(QUERY_DATASOURCE_TYPE, name));
    }

    public static MaterializedViewValidationException viewDataSourceInvalid(String name, Set<SourceType> nonExistSourceTypes) {
        return new MaterializedViewValidationException(String.format(VIEW_DATASOURCE_TYPE, name, nonExistSourceTypes));
    }
}
