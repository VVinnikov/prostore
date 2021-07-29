package io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdpTableEntity {

    private String name;
    private String schema;
    private List<AdpTableColumn> columns;
    private List<String> primaryKeys;

}
