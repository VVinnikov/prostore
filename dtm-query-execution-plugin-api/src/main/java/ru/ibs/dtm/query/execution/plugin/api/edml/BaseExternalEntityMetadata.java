package ru.ibs.dtm.query.execution.plugin.api.edml;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.common.plugin.exload.Format;

@Data
@AllArgsConstructor
public class BaseExternalEntityMetadata {
    private String name;
    private String locationPath;
    private Format format;
    private String externalSchema;
}
