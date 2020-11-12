package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdbCreateTableQueries {
    private final String createActualTableQuery;
    private final String createHistoryTableQuery;
    private final String createStagingTableQuery;
}
