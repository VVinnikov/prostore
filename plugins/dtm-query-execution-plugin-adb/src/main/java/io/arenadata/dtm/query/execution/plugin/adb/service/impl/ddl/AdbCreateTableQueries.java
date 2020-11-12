package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdbCreateTableQueries {
    private final String actual;
    private final String history;
    private final String staging;
}
