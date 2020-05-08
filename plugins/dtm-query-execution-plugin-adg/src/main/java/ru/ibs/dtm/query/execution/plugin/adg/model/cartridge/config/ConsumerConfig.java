package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.config;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Конфигурация консьюмера
 *
 * @custom_properties доп. свойства
 * @topics топики (записываем сюда название топика запроса)
 * @properties свойства
 */
@Data
public class ConsumerConfig {

  public static final String FILE_NAME = "kafka.consumers.yml";

  LinkedHashMap<?,?> custom_properties;
  Set<String> topics;
  LinkedHashMap<?,?> properties;
}
