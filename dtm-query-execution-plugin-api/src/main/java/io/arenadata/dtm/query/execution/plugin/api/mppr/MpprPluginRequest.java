package io.arenadata.dtm.query.execution.plugin.api.mppr;

import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.UUID;

@Getter
@ToString
public class MpprPluginRequest extends PluginRequest {
    private final MpprRequest mpprRequest;
    private final SqlNode sqlNode;

    public MpprPluginRequest(UUID requestId,
                             String envName,
                             String datamartMnemonic,
                             SqlNode sqlNode,
                             MpprRequest mpprRequest) {
        super(requestId, envName, datamartMnemonic);
        this.mpprRequest = mpprRequest;
        this.sqlNode = sqlNode;
    }
}
