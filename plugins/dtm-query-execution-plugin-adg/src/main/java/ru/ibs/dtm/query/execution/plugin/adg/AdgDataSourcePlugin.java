package ru.ibs.dtm.query.execution.plugin.adg;

import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdgDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> llrService,
            MpprKafkaService<QueryResult> mpprKafkaService,
            MppwKafkaService<QueryResult> mppwKafkaService,
            QueryCostService<Integer> adgQueryCostService,
            StatusService<StatusQueryResult> statusService) {
        super(ddlService, llrService, mpprKafkaService, mppwKafkaService, adgQueryCostService, statusService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADG;
    }

}
