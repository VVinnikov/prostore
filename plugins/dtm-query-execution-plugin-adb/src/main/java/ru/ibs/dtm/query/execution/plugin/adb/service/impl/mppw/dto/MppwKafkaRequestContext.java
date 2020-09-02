package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MppwKafkaRequestContext {
    private RestLoadRequest restLoadRequest;
    private MppwTransferDataRequest mppwTransferDataRequest;
}
