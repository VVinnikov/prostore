package io.arenadata.dtm.query.execution.core.configuration.properties;


import io.arenadata.dtm.common.reader.SourceType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("core.datasource.edml")
@Data
public class EdmlProperties {
    private SourceType sourceType;
    private Integer defaultChunkSize;
    private Integer defaultMessageLimit;
    private Integer pluginStatusCheckPeriodMs;
    private Integer firstOffsetTimeoutMs;
    private Integer changeOffsetTimeoutMs;
}
