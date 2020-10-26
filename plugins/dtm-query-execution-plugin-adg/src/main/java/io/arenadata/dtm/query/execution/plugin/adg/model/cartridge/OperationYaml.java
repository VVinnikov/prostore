package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import lombok.Data;

import java.util.LinkedHashMap;


/**
 * Список пространств
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationYaml {
    LinkedHashMap<String, Space> spaces;
}
