package io.arenadata.dtm.query.execution.plugin.adb;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdbDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adbLlrService,
            MpprKafkaService<QueryResult> adbMpprKafkaService,
            MppwKafkaService<QueryResult> adbMppwKafkaService,
            QueryCostService<Integer> adbQueryCostService,
            StatusService<StatusQueryResult> statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService) {
        super(ddlService,
                adbLlrService,
                adbMpprKafkaService,
                adbMppwKafkaService,
                adbQueryCostService,
                statusService,
                rollbackService,
                checkTableService, checkDataService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADB;
    }

}
