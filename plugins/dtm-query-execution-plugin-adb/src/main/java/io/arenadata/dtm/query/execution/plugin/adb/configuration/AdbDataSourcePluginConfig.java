package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
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
public class AdbDataSourcePluginConfig {

    @Bean("adbDtmDataSourcePlugin")
    public AdbDtmDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adbDdlService") DdlService<Void> ddlService,
            @Qualifier("adbLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adbMpprService") MpprService mpprService,
            @Qualifier("adbMppwService") MppwService mppwService,
            @Qualifier("adbStatusService") StatusService statusService,
            @Qualifier("adbRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adbCheckTableService") CheckTableService checkTableService,
            @Qualifier("adbCheckDataService") CheckDataService checkDataService,
            @Qualifier("adbTruncateHistoryService") TruncateHistoryService truncateHistoryService) {
        return new AdbDtmDataSourcePlugin(
                ddlService,
                llrService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService);
    }
}
