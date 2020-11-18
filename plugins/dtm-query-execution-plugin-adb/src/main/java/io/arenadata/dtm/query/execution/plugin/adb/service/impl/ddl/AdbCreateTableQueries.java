package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class AdbCreateTableQueries {
    private final String actual;
    private final String history;
    private final String staging;
    private final AdbTableEntity actualEntity;
    private final AdbTableEntity historyEntity;
    private final AdbTableEntity stagingEntity;
}
