package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.plugin.properties.PluginSelectCategoryProperties;
import io.arenadata.dtm.query.execution.core.service.dml.SuitablePluginSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
public class SuitablePluginSelectorImpl implements SuitablePluginSelector {

    private final PluginSelectCategoryProperties pluginSelectCategoryProperties;

    @Autowired
    public SuitablePluginSelectorImpl(PluginSelectCategoryProperties pluginSelectCategoryProperties) {
        this.pluginSelectCategoryProperties = pluginSelectCategoryProperties;
    }

    @Override
    public Optional<SourceType> selectByCategory(SelectCategory category, Set<SourceType> acceptablePlugins) {
        List<SourceType> prioritySourceTypes = pluginSelectCategoryProperties.getMapping().get(category);
        for (SourceType sourceType: prioritySourceTypes) {
            if (acceptablePlugins.contains(sourceType)) {
                return Optional.of(sourceType);
            }
        }
        return Optional.empty();
    }
}
