package io.arenadata.dtm.query.execution.plugin.api.mppr.kafka;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class DownloadExternalEntityMetadata extends BaseExternalEntityMetadata {
    private Integer chunkSize;

    @Builder
    public DownloadExternalEntityMetadata(String name, String locationPath, ExternalTableFormat format,
                                          String externalSchema, Integer chunkSize) {
        super(name, locationPath, format, externalSchema);
        this.chunkSize = chunkSize;
    }
}
