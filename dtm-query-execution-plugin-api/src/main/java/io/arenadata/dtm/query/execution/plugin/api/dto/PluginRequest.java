package io.arenadata.dtm.query.execution.plugin.api.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.UUID;

@Getter
@AllArgsConstructor
public class PluginRequest {
    private UUID requestId;
    private String envName;
    private String datamartMnemonic;
}
