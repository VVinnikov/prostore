package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExternalEntityMetadata extends BaseExternalEntityMetadata {
    private Integer uploadMessageLimit;

    @Builder
    public UploadExternalEntityMetadata(String name, String locationPath,
                                        ExternalTableFormat format, String externalSchema, Integer uploadMessageLimit) {
        super(name, locationPath, format, externalSchema);
        this.uploadMessageLimit = uploadMessageLimit;
    }
}
