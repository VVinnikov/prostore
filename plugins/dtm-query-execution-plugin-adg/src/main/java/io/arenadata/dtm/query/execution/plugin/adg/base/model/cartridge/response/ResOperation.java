package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response;

import lombok.Data;

import java.util.List;

/**
 * Operation result
 *
 * @data data
 * @errors error list
 */
@Data
public class ResOperation {
    ResData data;
    List<ResError> errors;
}

