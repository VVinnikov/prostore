package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class MppwKafkaRequestContext implements Serializable {
    private MppwKafkaLoadRequest mppwKafkaLoadRequest;
    private MppwTransferDataRequest mppwTransferDataRequest;
}
