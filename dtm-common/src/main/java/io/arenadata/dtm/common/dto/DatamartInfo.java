package io.arenadata.dtm.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor
public class DatamartInfo {
    private String schemaName;
    private Set<String> tables;
}
