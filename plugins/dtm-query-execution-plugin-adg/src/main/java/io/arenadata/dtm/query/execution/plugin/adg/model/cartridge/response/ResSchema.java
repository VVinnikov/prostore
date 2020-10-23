package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Возврат схемы
 */
@Data
public class ResSchema {
  @JsonProperty("as_yaml")
  String yaml;
}
