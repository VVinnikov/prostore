package io.arenadata.dtm.query.execution.plugin.adb.check.factory;

import io.arenadata.dtm.common.model.ddl.EntityField;

public interface AdbCheckDataByHashFieldValueFactory {

    String create(EntityField field);
}
