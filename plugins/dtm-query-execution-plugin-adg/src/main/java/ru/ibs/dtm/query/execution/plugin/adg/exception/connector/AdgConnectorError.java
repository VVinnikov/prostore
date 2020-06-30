package ru.ibs.dtm.query.execution.plugin.adg.exception.connector;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class AdgConnectorError extends RuntimeException {
    private String code;
    private String message;

    @Override
    public String getMessage() {
        return String.format("Code: [%s], message: [%s]", code, message);
    }
}
