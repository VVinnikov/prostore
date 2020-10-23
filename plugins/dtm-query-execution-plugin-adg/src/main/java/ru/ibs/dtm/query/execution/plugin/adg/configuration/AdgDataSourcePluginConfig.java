package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

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
            @Qualifier("adgRollbackService") RollbackService<Void> rollbackService) {
        return new AdgDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                queryCostService,
                statusService,
                rollbackService);
    }
}
