package io.arenadata.dtm.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerInfo implements Serializable {
  private String host;
  private int port;

  public String getAddress() {
    return host + ":" + port;
  }
}
