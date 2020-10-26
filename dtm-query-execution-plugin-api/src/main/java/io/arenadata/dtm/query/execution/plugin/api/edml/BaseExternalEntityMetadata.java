package io.arenadata.dtm.query.execution.plugin.api.edml;

import io.arenadata.dtm.common.plugin.exload.Format;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BaseExternalEntityMetadata {
    private String name;
    private String locationPath;
    private Format format;
    private String externalSchema;
}
