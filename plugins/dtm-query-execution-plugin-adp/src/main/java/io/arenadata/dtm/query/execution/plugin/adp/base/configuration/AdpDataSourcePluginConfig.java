package io.arenadata.dtm.query.execution.plugin.adp.base.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adp.base.service.AdpDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdpDataSourcePluginConfig {

    @Bean("adpDtmDataSourcePlugin")
    public AdpDtmDataSourcePlugin adpDataSourcePlugin(
            @Qualifier("adpDdlService") DdlService<Void> ddlService,
            @Qualifier("adpLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adpMpprService") MpprService mpprService,
            @Qualifier("adpMppwService") MppwService mppwService,
            @Qualifier("adpStatusService") StatusService statusService,
            @Qualifier("adpRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adpCheckTableService") CheckTableService checkTableService,
            @Qualifier("adpCheckDataService") CheckDataService checkDataService,
            @Qualifier("adpTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adpCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adpInitializationService") PluginInitializationService initializationService,
            @Qualifier("adpSynchronizeService") SynchronizeService synchronizeService) {
        return new AdpDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService,
                checkVersionService,
                initializationService,
                synchronizeService);
    }
}
