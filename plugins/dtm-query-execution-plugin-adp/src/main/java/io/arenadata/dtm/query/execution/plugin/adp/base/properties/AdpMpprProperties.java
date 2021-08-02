package io.arenadata.dtm.query.execution.plugin.adp.base.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adp.mppr")
@Component
public class AdpMpprProperties {
    private String restLoadUrl;
    private String restVersionUrl;
}
