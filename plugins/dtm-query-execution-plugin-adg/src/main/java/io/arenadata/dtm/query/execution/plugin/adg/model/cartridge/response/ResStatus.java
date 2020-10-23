package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;

import java.util.Map;

/**
 * Статус выполнения запроса
 */
@Data
public class ResStatus {
  private ResStatusEnum status;
  private String errorCode;
  private String message;
  private String error;
  private Map<String, String> opts;
}
