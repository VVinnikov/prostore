package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class PrepareLlrRequest extends PluginRequest {

    public PrepareLlrRequest(UUID requestId,
                             String envName,
                             String datamartMnemonic) {
        super(requestId, envName, datamartMnemonic);
    }
}
