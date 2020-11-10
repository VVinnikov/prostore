package io.arenadata.dtm.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerInfo {
  private String host;
  private int port;

  public String getAddress() {
    return host + ":" + port;
  }
}
