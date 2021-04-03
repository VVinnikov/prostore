package io.arenadata.dtm.query.execution.core.check.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.version.VersionInfo;

import java.util.List;

public interface CheckVersionQueryResultFactory {

    QueryResult create(List<VersionInfo> versionInfos);
}
