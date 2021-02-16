package io.arenadata.dtm.query.execution.plugin.api.edml;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BaseExternalEntityMetadata {
    private String name;
    private String locationPath;
    private ExternalTableFormat format;
    private String externalSchema;
}
