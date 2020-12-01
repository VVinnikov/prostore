package io.arenadata.dtm.query.execution.plugin.api.dto;


import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PluginParams {
    private final SourceType sourceType;
    private final RequestMetrics requestMetrics;
}
