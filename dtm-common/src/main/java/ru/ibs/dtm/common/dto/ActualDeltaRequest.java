package ru.ibs.dtm.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * DTO поиска актуальной дельты витрины на дату
 */
@Data
@AllArgsConstructor
public class ActualDeltaRequest {
    /**
     * Витрина
     */
    private String datamart;
    /**
     * Datatime в формате 2019-12-23 15:15:14
     */
    private String dateTime;
    /**
     * Признак для получения последней незакомиченной дельты
     */
    private boolean isLatestUncommitedDelta;
}
