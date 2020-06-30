package ru.ibs.dtm.query.execution.plugin.adg.exception.connector;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AdgLoadDataConnectorError extends AdgConnectorError {
    private long messageCount;
}
