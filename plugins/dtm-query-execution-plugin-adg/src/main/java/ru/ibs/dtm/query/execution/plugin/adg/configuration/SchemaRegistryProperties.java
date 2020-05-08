package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки подключения Schema Registry
 *
 * @url URL
 */
@Data
@ConfigurationProperties("schema-registry")
public class SchemaRegistryProperties {
  String url;
}
