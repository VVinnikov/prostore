package ru.ibs.dtm.query.execution.plugin.api.mppr.kafka;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;

@EqualsAndHashCode(callSuper = true)
@Data
public class DownloadExternalEntityMetadata extends BaseExternalEntityMetadata {
    private Integer chunkSize;

    @Builder
    public DownloadExternalEntityMetadata(String name, String locationPath, Format format,
                                          String externalSchema, Integer chunkSize) {
        super(name, locationPath, format, externalSchema);
        this.chunkSize = chunkSize;
    }
}
