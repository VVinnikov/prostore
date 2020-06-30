package ru.ibs.dtm.query.execution.plugin.adg.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdgHelperTableNames {
    private String staging;
    private String history;
    private String actual;
}
