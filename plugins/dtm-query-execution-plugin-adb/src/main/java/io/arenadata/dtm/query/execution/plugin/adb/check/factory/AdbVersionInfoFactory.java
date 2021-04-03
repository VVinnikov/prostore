package io.arenadata.dtm.query.execution.plugin.adb.check.factory;

import io.arenadata.dtm.common.version.VersionInfo;

import java.util.List;
import java.util.Map;

public interface AdbVersionInfoFactory {

    List<VersionInfo> create(List<Map<String, Object>> rows);
}
