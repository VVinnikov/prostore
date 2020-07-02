package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TtLoadDataKafkaError extends TtKafkaError {
    private long messageCount;
}
