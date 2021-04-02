package io.arenadata.dtm.query.execution.plugin.adqm.check.factory;

import io.arenadata.dtm.common.version.VersionInfo;

import java.util.List;
import java.util.Map;

public interface AdqmVersionInfoFactory {

    List<VersionInfo> create(List<Map<String, Object>> rows);
}
