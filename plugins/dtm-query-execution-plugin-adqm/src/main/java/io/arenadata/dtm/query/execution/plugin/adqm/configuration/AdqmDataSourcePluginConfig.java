package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.AdqmDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
