package io.arenadata.dtm.query.execution.plugin.api.dto;


import lombok.Data;

import java.util.UUID;

@Data
public class PluginRequest {
    protected final UUID requestId;
    protected final String envName;
    protected final String datamartMnemonic;
}
