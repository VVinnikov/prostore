package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Configuration
public class AdbDataSourcePluginConfig {

    @Bean("adbDtmDataSourcePlugin")
    public AdbDtmDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adbDdlService") DdlService<Void> ddlService,
            @Qualifier("adbLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adbMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adbMppwKafkaService") MppwKafkaService<QueryResult> mppwKafkaService,
            @Qualifier("adbQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adbStatusService") StatusService<StatusQueryResult> statusService,
            @Qualifier("adbRollbackService") RollbackService<Void> rollbackService) {
        return new AdbDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                queryCostService,
                statusService,
                rollbackService);
    }
}
