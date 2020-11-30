package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface CheckTableService extends DatamartExecutionService<CheckContext, AsyncResult<Void>> {
    String COLUMN_NAME = "column_name";
    String DATA_TYPE = "data_type";
    String FIELD_ERROR_TEMPLATE = "\t\t`%s` expected `%s` got `%s`.";
    String TABLE_NOT_EXIST_ERROR_TEMPLATE = "Table ‘%s’ does not exist.";
    String COLUMN_NOT_EXIST_ERROR_TEMPLATE = "\tColumn '%s' does not exist.";

    void check(CheckContext context, Handler<AsyncResult<Void>> handler);

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }

    default void execute(CheckContext context, Handler<AsyncResult<Void>> handler) {
        check(context, handler);
    }
}
