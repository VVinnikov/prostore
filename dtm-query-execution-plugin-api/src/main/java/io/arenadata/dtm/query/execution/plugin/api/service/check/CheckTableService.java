package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.vertx.core.Future;

public interface CheckTableService {
    String FIELD_ERROR_TEMPLATE = "\t\t`%s` expected `%s` got `%s`.";
    String TABLE_NOT_EXIST_ERROR_TEMPLATE = "Table ‘%s’ does not exist.";
    String COLUMN_NOT_EXIST_ERROR_TEMPLATE = "\tColumn '%s' does not exist.";

    Future<Void> check(CheckContext context);
}
