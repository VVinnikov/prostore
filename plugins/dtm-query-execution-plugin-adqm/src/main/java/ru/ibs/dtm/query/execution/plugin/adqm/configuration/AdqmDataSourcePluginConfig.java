package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.AdqmDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Configuration
public class AdqmDataSourcePluginConfig {

    @Bean("adqmDtmDataSourcePlugin")
    public AdqmDtmDataSourcePlugin adqmDataSourcePlugin(
            @Qualifier("adqmDdlService") DdlService<Void> ddlService,
            @Qualifier("adqmLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adqmMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
            @Qualifier("adqmQueryCostService") QueryCostService<Integer> queryCostService) {
        return new AdqmDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprKafkaService,
                queryCostService);
    }
}
