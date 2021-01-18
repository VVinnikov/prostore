package io.arenadata.dtm.query.execution.plugin.api.mppr;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.MPPR;

@Getter
@ToString
public class MpprRequestContext extends RequestContext<MpprRequest> {
    private List<DeltaInformation> deltaInformations;

    public MpprRequestContext(RequestMetrics metrics,
                              MpprRequest request,
                              SqlNode query,
                              List<DeltaInformation> deltaInformations) {
        super(request, sqlNode, envName, metrics);
        this.deltaInformations = deltaInformations;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return MPPR;
    }
}
