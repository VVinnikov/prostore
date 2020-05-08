package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;

import java.util.List;

/**
 * Данные по кластеру
 *
 * @schema схема
 * @config конфигурации
 */
@Data
public class ResCluster {
  ResSchema schema;
  List<ResConfig> config;
}
