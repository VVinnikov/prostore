package ru.ibs.dtm.query.execution.core.configuration.properties;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.SourceType;

@Component
@ConfigurationProperties("datasource.edml")
public class EdmlProperties {

  private SourceType sourceType;
  private Integer defaultChunkSize;

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  public Integer getDefaultChunkSize() {
    return defaultChunkSize;
  }

  public void setDefaultChunkSize(Integer defaultChunkSize) {
    this.defaultChunkSize = defaultChunkSize;
  }
}
