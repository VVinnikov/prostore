package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("tarantool.cartridge")
public class TarantoolCartridgeProperties {

  String adminApiUrl = "/admin/api";

  String sendQueryUrl = "/api/kafka/send_query";

  String url;
}
