package io.arenadata.dtm.query.execution.plugin.adp.base.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("adp.mppr")
@Data
public class AdpMpprProperties {
    private String restVersionUrl;
}
