package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import lombok.Data;

import java.util.LinkedHashMap;


/**
 * Список пространств
 */
@Data
public class OperationYaml {
  LinkedHashMap<String, Space> spaces;
}
