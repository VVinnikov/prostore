package io.arenadata.dtm.query.execution.plugin.adg;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADG_DATAMART_CACHE = "adg_datamart";
    public static final String ADG_QUERY_TEMPLATE_CACHE = "adgQueryTemplateCache";

    public AdgDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> llrService,
            MpprKafkaService mpprKafkaService,
            MppwKafkaService mppwKafkaService,
            QueryCostService<Integer> adgQueryCostService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateHistoryService) {
        super(ddlService,
                llrService,
                mpprKafkaService,
                mppwKafkaService,
                adgQueryCostService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService);
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
