package ru.ibs.dtm.query.execution.plugin.api.mppw.parameter;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaParameter {
    private String datamart;
    private long sysCn;
    private String targetTableName;
    private UploadExternalMetadata uploadMetadata;
}
