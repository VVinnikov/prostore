package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class TtKafkaError extends RuntimeException {
    private String code;
    private String message;

    @Override
    public String getMessage() {
        return String.format("Code: [%s], message: [%s]", code, message);
    }
}
