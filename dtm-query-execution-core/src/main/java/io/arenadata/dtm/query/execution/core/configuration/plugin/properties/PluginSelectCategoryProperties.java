package io.arenadata.dtm.query.execution.core.configuration.plugin.properties;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Data
@ConfigurationProperties("core.plugins.category")
public class PluginSelectCategoryProperties {
    private Map<SelectCategory, List<SourceType>> mapping;
}
