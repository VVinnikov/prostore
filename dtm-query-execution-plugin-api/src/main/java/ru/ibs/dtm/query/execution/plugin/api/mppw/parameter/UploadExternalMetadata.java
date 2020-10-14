package ru.ibs.dtm.query.execution.plugin.api.mppw.parameter;

import lombok.Builder;
import lombok.Data;
import ru.ibs.dtm.common.plugin.exload.Format;

@Data
@Builder
public class UploadExternalMetadata {
    private String name;
    private String externalTableLocationPath;
    private Format externalTableFormat;
    private String externalTableSchema;
    private Integer externalTableUploadMessageLimit;
    private String zookeeperHost;
    private int zookeeperPort;
    private String topic;
}
