package ru.ibs.dtm.query.execution.plugin.adqm;

import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdqmDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdqmDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adqmLlrService,
            MpprKafkaService<QueryResult> adqmMpprKafkaService,
            MppwKafkaService<QueryResult> mppwKafkaService,
            QueryCostService<Integer> adqmQueryCostService,
            StatusService<StatusQueryResult> statusService) {
        super(ddlService, adqmLlrService, adqmMpprKafkaService, mppwKafkaService, adqmQueryCostService, statusService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADQM;
    }

}
