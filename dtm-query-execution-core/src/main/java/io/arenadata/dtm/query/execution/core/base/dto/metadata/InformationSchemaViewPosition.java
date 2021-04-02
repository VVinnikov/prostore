package io.arenadata.dtm.query.execution.core.base.dto.metadata;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The position of the information schema view in the request
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InformationSchemaViewPosition {

    /**
     * View
     */
    private InformationSchemaView view;

    /**
     * Start position
     */
    private int start;

    /**
     * End position
     */
    private int end;
}
