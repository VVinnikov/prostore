package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class GetRequest {
  private String requestUri;
  private Map<String, String> queryParamMap;
}
