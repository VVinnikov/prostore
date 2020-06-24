package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

@Service
@Slf4j
public class EdmlUploadExternalExecutor implements EdmlExecutor {

    @Override
    public void execute(EdmlRequestContext context, EdmlQuery edmlQuery, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        //TODO
    }

    @Override
    public EdmlAction getAction() {
        return EdmlAction.UPLOAD;
    }
}
