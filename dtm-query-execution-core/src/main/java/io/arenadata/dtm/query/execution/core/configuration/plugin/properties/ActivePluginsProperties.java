package io.arenadata.dtm.query.execution.core.configuration.plugin.properties;

import io.arenadata.dtm.common.reader.SourceType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@Data
@ConfigurationProperties("core.plugins")
public class ActivePluginsProperties {
    private Set<SourceType> active;
}
