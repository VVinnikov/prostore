package ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka;

import lombok.Data;

/**
 * Настройки топиков
 *
 * @adgUploadRq Топик запроса ADG
 * @adgUploadRs Топик ответа от запроса к ADG
 * @adgUploadErr Топик ошибок от запроса к ADG
 */
@Data
public class KafkaAdminProperty {
  String adgUploadRq = "";
  String adgUploadRs = "";
  String adgUploadErr = "";
}
