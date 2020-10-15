package ru.ibs.dtm.query.execution.plugin.api.mppw.kafka;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExternalEntityMetadata extends BaseExternalEntityMetadata {
    private Integer uploadMessageLimit;

    @Builder
    public UploadExternalEntityMetadata(String name, String locationPath,
                                        Format format, String externalSchema, Integer uploadMessageLimit) {
        super(name, locationPath, format, externalSchema);
        this.uploadMessageLimit = uploadMessageLimit;
    }
}
