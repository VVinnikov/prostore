package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;

import java.util.LinkedHashMap;


/**
 * Список пространств
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationYaml {
    LinkedHashMap<String, Space> spaces;
}
