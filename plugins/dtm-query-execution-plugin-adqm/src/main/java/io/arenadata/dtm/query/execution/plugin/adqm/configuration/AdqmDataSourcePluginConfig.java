package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.AdqmDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdqmDataSourcePluginConfig {

    @Bean("adqmDtmDataSourcePlugin")
    public AdqmDtmDataSourcePlugin adqmDataSourcePlugin(
            @Qualifier("adqmDdlService") DdlService<Void> ddlService,
            @Qualifier("adqmLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adqmMpprService") MpprService mpprService,
            @Qualifier("adqmMppwService") MppwService mppwService,
            @Qualifier("adqmQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adqmStatusService") StatusService statusService,
            @Qualifier("adqmRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adqmCheckTableService") CheckTableService checkTableService,
            @Qualifier("adqmCheckDataService") CheckDataService checkDataService,
            @Qualifier("adqmTruncateHistoryService") TruncateHistoryService truncateHistoryService) {
        return new AdqmDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprService,
                mppwService,
                queryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService);
    }
}
