package io.arenadata.dtm.query.execution.plugin.api.mppr.kafka;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

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
