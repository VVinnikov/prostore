package ru.ibs.dtm.query.execution.plugin.adg.service;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.config.ConsumerConfig;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.config.TopicsConfig;

import java.util.LinkedHashMap;

/**
 * Декодер/кодер строчки конфигурации
 */
public interface ContentWriter {
  String toContent(Object config);
  ConsumerConfig toConsumerConfig(String content);
  LinkedHashMap<String, TopicsConfig> toTopicsConfig(String content);
}
