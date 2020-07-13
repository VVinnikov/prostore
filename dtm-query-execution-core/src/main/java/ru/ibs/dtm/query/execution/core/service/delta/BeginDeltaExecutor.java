package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaExecutor implements DeltaExecutor {

    private ServiceDbFacade serviceDbFacade;
    private DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public BeginDeltaExecutor(ServiceDbFacade serviceDbFacade, DeltaQueryResultFactory deltaQueryResultFactory) {
        this.serviceDbFacade = serviceDbFacade;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        getDeltaHotByDatamart(context)
                .compose(newDelta -> insertNewDelta(context, newDelta))
                .onSuccess(success -> handler.handle(Future.succeededFuture(success)))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
    }

    private Future<DeltaRecord> getDeltaHotByDatamart(DeltaRequestContext context) {
        return Future.future((Promise<DeltaRecord> promiseDelta) ->
                serviceDbFacade.getDeltaServiceDao().getDeltaHotByDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), ar -> {
                    if (ar.succeeded()) {
                        DeltaRecord deltaRecord = ar.result();
                        log.debug("Найдена последняя delta: {} для витрины: {}", deltaRecord,
                                context.getRequest().getQueryRequest().getDatamartMnemonic());
                        Long deltaHot = initAndCheckDeltaHot(context, promiseDelta, deltaRecord);
                        log.debug("Найдена deltaHot: {} для витрины: {}", deltaHot,
                                context.getRequest().getQueryRequest().getDatamartMnemonic());
                        DeltaRecord newDelta = createNextDeltaRecord(deltaHot,
                                context.getRequest().getQueryRequest().getDatamartMnemonic());
                        promiseDelta.complete(newDelta);
                    } else {
                        promiseDelta.fail(ar.cause());
                    }
                }));
    }

    private Future<QueryResult> insertNewDelta(DeltaRequestContext context, DeltaRecord newDelta) {
        return Future.future((Promise<QueryResult> promiseDelta) ->
                serviceDbFacade.getDeltaServiceDao().insertDelta(newDelta, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Создана новая дельта: {} для витрины: {}", newDelta,
                                context.getRequest().getQueryRequest().getDatamartMnemonic());
                        QueryResult res = deltaQueryResultFactory.create(context, newDelta);
                        promiseDelta.complete(res);
                    } else {
                        promiseDelta.fail(ar.cause());
                    }
                }));
    }

    @Nullable
    private Long initAndCheckDeltaHot(DeltaRequestContext context, Promise<DeltaRecord> promiseDelta, DeltaRecord deltaRecord) {
        Long deltaHot = 0L;
        if (deltaRecord != null) {
            deltaHot = getDeltaHot(deltaRecord);
            if (deltaHot == null) {
                promiseDelta.fail(new RuntimeException("Дельта находится в процессе загрузки!"));
            } else if (((BeginDeltaQuery) context.getDeltaQuery()).getDeltaNum() != null
                    && !((BeginDeltaQuery) context.getDeltaQuery()).getDeltaNum().equals(deltaHot)) {
                promiseDelta.fail(new RuntimeException("Номера заданной дельты и актуальной не совпадают!"));
            }
        }
        return deltaHot;
    }

    private Long getDeltaHot(DeltaRecord deltaRecord) {
        switch (deltaRecord.getStatus()) {
            case SUCCESS:
                return deltaRecord.getSinId() + 1;
            case ERROR:
                return deltaRecord.getSinId();
            case IN_PROCESS:
            default:
                return null;
        }
    }

    @NotNull
    private DeltaRecord createNextDeltaRecord(Long deltaHot, String datamartMnemonic) {
        DeltaRecord newDelta = new DeltaRecord();
        newDelta.setSinId(deltaHot);
        newDelta.setDatamartMnemonic(datamartMnemonic);
        newDelta.setLoadProcId(UUID.randomUUID().toString());
        newDelta.setStatusDate(LocalDateTime.now());
        newDelta.setStatus(DeltaLoadStatus.IN_PROCESS);
        return newDelta;
    }

    @Override
    public DeltaAction getAction() {
        return BEGIN_DELTA;
    }
}
