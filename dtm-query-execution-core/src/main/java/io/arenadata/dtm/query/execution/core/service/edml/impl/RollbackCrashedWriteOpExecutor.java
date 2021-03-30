package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.factory.RollbackWriteOpsQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.service.rollback.RestoreStateService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RollbackCrashedWriteOpExecutor implements EdmlExecutor {

    private final RestoreStateService restoreStateService;
    private final RollbackWriteOpsQueryResultFactory writeOpsQueryResultFactory;

    @Autowired
    public RollbackCrashedWriteOpExecutor(RestoreStateService restoreStateService,
                                          RollbackWriteOpsQueryResultFactory writeOpsQueryResultFactory) {
        this.restoreStateService = restoreStateService;
        this.writeOpsQueryResultFactory = writeOpsQueryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        if (StringUtils.isEmpty(context.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must not be empty!";
            return Future.failedFuture(new DtmException(errMsg));
        } else {
            return restoreStateService.restoreErase(context.getRequest().getQueryRequest().getDatamartMnemonic())
                    .map(writeOpsQueryResultFactory::create);
        }
    }

    @Override
    public EdmlAction getAction() {
        return EdmlAction.ROLLBACK;
    }
}
