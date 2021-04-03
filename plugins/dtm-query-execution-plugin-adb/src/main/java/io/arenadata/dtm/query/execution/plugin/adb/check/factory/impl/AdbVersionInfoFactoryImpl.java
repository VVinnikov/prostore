package io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbVersionInfoFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactoryImpl.COMPONENT_NAME_COLUMN;
import static io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactoryImpl.VERSION_COLUMN;

@Service
public class AdbVersionInfoFactoryImpl implements AdbVersionInfoFactory {

    @Override
    public List<VersionInfo> create(List<Map<String, Object>> rows) {
        return rows.stream()
                .map(row -> new VersionInfo(row.get(COMPONENT_NAME_COLUMN).toString(),
                        row.get(VERSION_COLUMN).toString()))
                .collect(Collectors.toList());
    }
}
