package io.arenadata.dtm.query.execution.plugin.adb.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdbTables<T> {
    public static final String ACTUAL_TABLE_POSTFIX = "actual";
    public static final String HISTORY_TABLE_POSTFIX = "history";
    public static final String STAGING_TABLE_POSTFIX = "staging";

    private final T actual;
    private final T history;
    private final T staging;
}
