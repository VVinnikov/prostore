package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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