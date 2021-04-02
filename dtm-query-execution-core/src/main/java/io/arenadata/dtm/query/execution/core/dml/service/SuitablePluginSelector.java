package io.arenadata.dtm.query.execution.core.dml.service;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.reader.SourceType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface SuitablePluginSelector {
    // get one of the Plugins prioritized for SelectCategory
    Optional<SourceType> selectByCategory(SelectCategory category, Set<SourceType> acceptablePlugins);
}