package io.arenadata.dtm.query.execution.plugin.adqm;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.Collections;
import java.util.Set;

public class AdqmDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADQM_DATAMART_CACHE = "adqm_datamart";

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

    @Override
    public Set<String> getActiveCaches() {
        return Collections.singleton(ADQM_DATAMART_CACHE);
    }
}
