package io.arenadata.dtm.query.execution.plugin.adqm;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdqmDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdqmDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adqmLlrService,
            MpprKafkaService<QueryResult> adqmMpprKafkaService,
            MppwKafkaService<QueryResult> mppwKafkaService,
            QueryCostService<Integer> adqmQueryCostService,
            StatusService<StatusQueryResult> statusService,
            RollbackService<Void> rollbackService) {
        super(ddlService, adqmLlrService, adqmMpprKafkaService, mppwKafkaService, adqmQueryCostService, statusService, rollbackService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADQM;
    }

}
