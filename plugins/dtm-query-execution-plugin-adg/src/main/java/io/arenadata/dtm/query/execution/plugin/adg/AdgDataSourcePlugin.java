package io.arenadata.dtm.query.execution.plugin.adg;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADG_DATAMART_CACHE = "adg_datamart";
    public static final String ADG_QUERY_TEMPLATE_CACHE = "adgQueryTemplateCache";

    public AdgDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> llrService,
            MpprService mpprService,
            MppwService mppwService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateHistoryService,
            CheckVersionService checkVersionService,
            PluginInitializationService initializationService) {
        super(ddlService,
                llrService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                checkVersionService,
                truncateHistoryService,
                initializationService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADG;
    }

    @Override
    public Set<String> getActiveCaches() {
        return new HashSet<>(Arrays.asList(ADG_DATAMART_CACHE, ADG_QUERY_TEMPLATE_CACHE));
    }
}
