package ru.ibs.dtm.query.execution.core.configuration.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.SourceType;

@Component
@ConfigurationProperties("datasource.edml")
@Data
public class EdmlProperties {
  private SourceType sourceType;
  private Integer defaultChunkSize;
  private Integer defaultMessageLimit;
  private Integer pluginStatusCheckPeriodMs;
}
