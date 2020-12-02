package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdgDataSourcePluginConfig {

    @Bean("adgDtmDataSourcePlugin")
    public AdgDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adgDdlService") DdlService<Void> ddlService,
            @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adgMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adgMppwKafkaService") MppwKafkaService<QueryResult> mppwKafkaService,
            @Qualifier("adgQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adgStatusService") StatusService<StatusQueryResult> statusService,
            @Qualifier("adgRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adgCheckTableService") CheckTableService checkTableService,
            @Qualifier("adgCheckDataService") CheckDataService checkDataService) {
        return new AdgDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                queryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService);
    }
}
