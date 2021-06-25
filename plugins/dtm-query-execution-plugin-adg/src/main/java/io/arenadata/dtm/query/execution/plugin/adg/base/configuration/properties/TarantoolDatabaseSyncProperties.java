package io.arenadata.dtm.query.execution.plugin.adg.base.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ToString
@ConfigurationProperties(prefix = "adg.tarantool.db.sync")
public class TarantoolDatabaseSyncProperties {
    Long timeoutConnect = 5000L;
    Long timeoutRead = 5000L;
    Long timeoutRequest = 5000L;
}
