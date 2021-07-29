package io.arenadata.dtm.query.execution.plugin.adp.mppw.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class AdpTransferDataRequest {
    private Long sysCn;
    private String datamart;
    private String tableName;
    private List<String> allFields;
    private List<String> primaryKeys;
}
