package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TtLoadDataKafkaError extends TarantoolError {
    private long messageCount;
}
