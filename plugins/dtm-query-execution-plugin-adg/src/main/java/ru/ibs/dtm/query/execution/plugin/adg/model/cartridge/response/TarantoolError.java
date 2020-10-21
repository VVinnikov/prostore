package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TarantoolError extends RuntimeException {
    private String code;
    private String message;

    @Override
    public String getMessage() {
        return String.format("Code: [%s], message: [%s]", code, message);
    }
}
