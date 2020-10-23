package io.arenadata.dtm.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Actual delta request dto
 */
@Data
@AllArgsConstructor
public class ActualDeltaRequest {
    /**
     * Datamart
     */
    private String datamart;
    /**
     * Datatime in format: 2019-12-23 15:15:14
     */
    private String dateTime;
    /**
     * Is last uncommited delta
     */
    private boolean isLatestUncommitedDelta;
}
