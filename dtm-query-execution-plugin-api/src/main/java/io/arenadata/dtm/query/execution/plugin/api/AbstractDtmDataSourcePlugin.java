package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.vertx.core.Future;

import java.util.List;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final MpprService mpprService;
    protected final MppwService mppwService;
    protected final StatusService statusService;
    protected final RollbackService<Void> rollbackService;
    protected final CheckTableService checkTableService;
    protected final CheckDataService checkDataService;
    protected final CheckVersionService checkVersionService;
    protected final TruncateHistoryService truncateService;

    public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       MpprService mpprService,
                                       MppwService mppwService,
                                       StatusService statusService,
                                       RollbackService<Void> rollbackService,
                                       CheckTableService checkTableService,
                                       CheckDataService checkDataService,
                                       CheckVersionService checkVersionService,
                                       TruncateHistoryService truncateService) {
        this.ddlService = ddlService;
        this.llrService = llrService;
        this.mpprService = mpprService;
        this.mppwService = mppwService;
        this.statusService = statusService;
        this.rollbackService = rollbackService;
        this.checkTableService = checkTableService;
        this.checkDataService = checkDataService;
        this.checkVersionService = checkVersionService;
        this.truncateService = truncateService;
    }

    @Override
    public Future<Void> ddl(DdlRequest request) {
        return ddlService.execute(request);
    }

    @Override
    public Future<QueryResult> llr(LlrRequest request) {
        return llrService.execute(request);
    }

    @Override
    public Future<Void> prepareLlr(LlrRequest request) {
        return llrService.prepare(request);
    }

    @Override
    public Future<QueryResult> mppr(MpprRequest request) {
        return mpprService.execute(request);
    }

    @Override
    public Future<QueryResult> mppw(MppwRequest request) {
        return mppwService.execute(request);
    }

    @Override
    public Future<StatusQueryResult> status(String topic) {
        return statusService.execute(topic);
    }

    @Override
    public Future<Void> rollback(RollbackRequest request) {
        return rollbackService.execute(request);
    }

    @Override
    public Future<Void> checkTable(CheckTableRequest request) {
        return checkTableService.check(request);
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return checkDataService.checkDataByCount(request);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params) {
        return checkDataService.checkDataByHashInt32(params);
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return checkVersionService.checkVersion(request);
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return truncateService.truncateHistory(request);
    }

}
