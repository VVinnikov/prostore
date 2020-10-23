package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;

import java.util.Map;

/**
 * Ошибка
 */
@Data
public class ResError {
    String message;
    Map<String, String> extensions;
}

