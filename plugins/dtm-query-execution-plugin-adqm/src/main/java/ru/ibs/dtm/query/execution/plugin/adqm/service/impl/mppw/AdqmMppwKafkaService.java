package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

@Slf4j
@RequiredArgsConstructor
@Service("adqmMppwKafkaService")
public class AdqmMppwKafkaService implements MppwKafkaService<QueryResult> {

    @Override
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        MppwRequest request = context.getRequest();
        //TODO реализовать
    }

}
