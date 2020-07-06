package ru.ibs.dtm.query.execution.plugin.adb;

import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public AdbDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adbLlrService,
            MpprKafkaService<QueryResult> adbMpprKafkaService,
            MppwKafkaService<QueryResult> adbMppwKafkaService,
            QueryCostService<Integer> adbQueryCostService,
            StatusService<StatusQueryResult> statusService) {
        super(ddlService, adbLlrService, adbMpprKafkaService, adbMppwKafkaService, adbQueryCostService, statusService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADB;
    }

}
