package io.arenadata.dtm.query.execution.plugin.adg;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdgDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> llrService,
            MpprKafkaService<QueryResult> mpprKafkaService,
            MppwKafkaService<QueryResult> mppwKafkaService,
            QueryCostService<Integer> adgQueryCostService,
            StatusService<StatusQueryResult> statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService) {
        super(ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                adgQueryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADG;
    }

}
