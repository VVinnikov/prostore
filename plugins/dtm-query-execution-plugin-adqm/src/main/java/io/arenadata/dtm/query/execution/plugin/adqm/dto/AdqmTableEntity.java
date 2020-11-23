package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdqmTableEntity {
    public String env;
    public String schema;
    public String name;
    public List<AdqmTableColumn> columns;
    public List<String> sortedKeys;
    public List<String> shardingKeys;

}
