package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TtLoadDataKafkaResponse {
    private long messageCount;
}
