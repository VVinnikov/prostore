package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.*;

@Configuration
public class AdgPluginConfig {

    @Bean("adgDtmDataSourcePlugin")
    public AdgDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adgDdlService") DdlService<Void> ddlService,
            @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adgMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adgMppwKafkaService") MppwKafkaService<QueryResult> mppwKafkaService,
            @Qualifier("adgQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adgKafkaStatusService") StatusService<StatusQueryResult> statusService) {
        return new AdgDataSourcePlugin(ddlService, llrService, mpprKafkaService, mppwKafkaService, queryCostService, statusService);
    }
}
