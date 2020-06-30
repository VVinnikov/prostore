package ru.ibs.dtm.query.execution.plugin.adqm;

import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

public class AdqmDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdqmDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adqmLlrService,
            MpprKafkaService<QueryResult> adqmMpprKafkaService,
            QueryCostService<Integer> adqmQueryCostService) {
        super(ddlService, adqmLlrService, adqmMpprKafkaService, adqmQueryCostService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADQM;
    }

}
