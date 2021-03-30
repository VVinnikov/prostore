package io.arenadata.dtm.query.execution.plugin.adqm;

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

import java.util.Collections;
import java.util.Set;

public class AdqmDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADQM_DATAMART_CACHE = "adqm_datamart";
    public static final String ADQM_QUERY_TEMPLATE_CACHE = "adqmQueryTemplateCache";

    public AdqmDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adqmLlrService,
            MpprService adqmMpprService,
            MppwService mppwService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateHistoryService,
            CheckVersionService checkVersionService) {
        super(ddlService,
                adqmLlrService,
                adqmMpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                checkVersionService,
                truncateHistoryService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADQM;
    }

    @Override
    public Set<String> getActiveCaches() {
        return Collections.singleton(ADQM_DATAMART_CACHE);
    }
}
