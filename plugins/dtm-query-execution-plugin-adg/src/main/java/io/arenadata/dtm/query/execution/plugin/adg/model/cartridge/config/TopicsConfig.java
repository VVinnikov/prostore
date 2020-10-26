package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Конфигурации топиков
 *
 * @error_topic топик ошибок
 * @schema_key название схемы
 * @target_table таблица
 * @schema_data схема из Schema Registry
 * @success_topic топик успешного выполнения запроса
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicsConfig {

  public static final String FILE_NAME = "kafka.topics.yml";

  @JsonProperty("error_topic")
  String errorTopic;
  @JsonProperty("schema_key")
  String schemaKey;
  @JsonProperty("target_table")
  String targetTable;
  @JsonProperty("schema_data")
  String schemaData;
  @JsonProperty("success_topic")
  String successTopic;
}
