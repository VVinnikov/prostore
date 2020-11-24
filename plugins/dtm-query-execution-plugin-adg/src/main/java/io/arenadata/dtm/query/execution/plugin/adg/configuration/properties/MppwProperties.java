package io.arenadata.dtm.query.execution.plugin.adg.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adg.mppw")
@Component
public class MppwProperties {
    private String consumerGroup;
    private long maxNumberOfMessagesPerPartition;
    private String callbackFunctionName = "transfer_data_to_scd_table_on_cluster_cb";
    private long callbackFunctionSecIdle;
}
