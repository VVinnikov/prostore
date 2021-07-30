package io.arenadata.dtm.query.execution.plugin.adp.mppw.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AdpConnectorMppwStopRequest implements Serializable {
    private String requestId;
    private String kafkaTopic;
}
