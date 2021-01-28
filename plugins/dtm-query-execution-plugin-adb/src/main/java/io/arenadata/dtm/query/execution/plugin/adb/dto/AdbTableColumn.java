package io.arenadata.dtm.query.execution.plugin.adb.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdbTableColumn {
    private final String name;
    private final String type;
    private final Boolean nullable;
}
