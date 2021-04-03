package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response;

import lombok.Data;

import java.util.Map;

/**
 * Error
 */
@Data
public class ResError {
    String message;
    Map<String, String> extensions;
}

