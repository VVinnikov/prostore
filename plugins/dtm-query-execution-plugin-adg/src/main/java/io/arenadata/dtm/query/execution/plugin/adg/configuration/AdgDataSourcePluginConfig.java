package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
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
public class AdgDataSourcePluginConfig {

    @Bean("adgDtmDataSourcePlugin")
    public AdgDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adgDdlService") DdlService<Void> ddlService,
            @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adgMpprService") MpprService mpprService,
            @Qualifier("adgMppwService") MppwService mppwService,
            @Qualifier("adgQueryCostService") QueryCostService<Integer> queryCostService,
            @Qualifier("adgStatusService") StatusService statusService,
            @Qualifier("adgRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adgCheckTableService") CheckTableService checkTableService,
            @Qualifier("adgCheckDataService") CheckDataService checkDataService,
            @Qualifier("adgTruncateHistoryService") TruncateHistoryService truncateHistoryService) {
        return new AdgDataSourcePlugin(
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
