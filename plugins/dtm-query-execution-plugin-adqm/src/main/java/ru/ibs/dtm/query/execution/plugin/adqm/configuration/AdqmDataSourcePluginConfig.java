package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.AdqmDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Configuration
public class AdqmDataSourcePluginConfig {

    @Bean("adqmDtmDataSourcePlugin")
    public AdqmDtmDataSourcePlugin adqmDataSourcePlugin(
            @Qualifier("adqmDdlService") DdlService<Void> ddlService,
            @Qualifier("adqmLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adqmMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adqmMppwKafkaService") MppwKafkaService<QueryResult> mppwKafkaService,
            @Qualifier("adqmQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adqmStatusService") StatusService<StatusQueryResult> statusService,
            @Qualifier("adqmRollbackService") RollbackService<Void> rollbackService) {
        return new AdqmDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                queryCostService,
                statusService,
                rollbackService);
    }
}
