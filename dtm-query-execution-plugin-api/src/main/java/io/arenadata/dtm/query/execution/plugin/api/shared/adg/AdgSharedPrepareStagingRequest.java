package io.arenadata.dtm.query.execution.plugin.api.shared.adg;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Data;

@Data
public class AdgSharedPrepareStagingRequest {
    private final String env;
    private final String datamart;
    private final Entity entity;
}
