package io.arenadata.dtm.query.execution.plugin.adb.check.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;

public interface AdbCheckDataQueryFactory {

    String createCheckDataByCountQuery(CheckDataByCountRequest request, String resultColumnName);

    String createCheckDataByHashInt32Query(CheckDataByHashInt32Request request, String resultColumnName);

}
