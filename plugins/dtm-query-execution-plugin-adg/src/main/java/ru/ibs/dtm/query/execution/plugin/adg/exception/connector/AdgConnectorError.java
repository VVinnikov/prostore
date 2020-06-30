package ru.ibs.dtm.query.execution.plugin.adg.exception.connector;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AdgConnectorError extends RuntimeException {
    private String code;
    private String message;
}
