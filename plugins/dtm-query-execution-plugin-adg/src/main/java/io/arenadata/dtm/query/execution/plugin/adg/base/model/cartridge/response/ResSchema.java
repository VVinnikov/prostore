package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Schema result
 */
@Data
public class ResSchema {
    @JsonProperty("as_yaml")
    String yaml;
}
