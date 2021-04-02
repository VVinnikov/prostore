package io.arenadata.dtm.query.execution.plugin.adg.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdgHelperTableNames {
    private String staging;
    private String history;
    private String actual;
    private String prefix;
}
