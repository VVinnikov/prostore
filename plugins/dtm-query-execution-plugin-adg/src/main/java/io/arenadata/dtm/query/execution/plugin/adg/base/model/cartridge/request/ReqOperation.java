package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable.Variables;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Base operation
 *
 * @operationName name
 * @variables variables
 * @query request
 */
@Data
@AllArgsConstructor
public abstract class ReqOperation {
    String operationName;
    Variables variables;
    String query;
}
