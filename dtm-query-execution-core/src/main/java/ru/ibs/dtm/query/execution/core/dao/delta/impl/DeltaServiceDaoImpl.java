package ru.ibs.dtm.query.execution.core.dao.delta.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.query.execution.core.dao.delta.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;
import static org.jooq.generated.dtmservice.Tables.DELTA_DATA;
import static org.jooq.impl.DSL.max;

@Repository
@Slf4j
public class DeltaServiceDaoImpl implements DeltaServiceDao {

    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();
    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DeltaServiceDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler) {
        final String datamart = actualDeltaRequest.getDatamart();
        final String dateTime = actualDeltaRequest.getDateTime();
        log.debug("Получение дельты витрины {} на {}, начало", datamart, dateTime);
        executor.query(dsl -> getDeltaByDatamartAndDateSelect(dsl, actualDeltaRequest)).setHandler(ar -> {
            if (ar.succeeded()) {
                final Long delta = ar.result().get(0, Long.class);
                log.debug("Дельта витрины {} на дату {}: {}", datamart, dateTime, delta);
                resultHandler.handle(Future.succeededFuture(delta));
            } else {
                log.error("Невозможно получить дельту витрины {} на дату {}: {}",
                        datamart, dateTime, ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Select<Record1<Long>> getUnionOfDeltaByDatamartAndDateSelects(DSLContext dsl, List<ActualDeltaRequest> actualDeltaRequests) {
        return actualDeltaRequests.stream()
                .map(adr -> getDeltaByDatamartAndDateSelect(dsl, adr))
                .reduce(Select::unionAll)
                .get();
    }

    private Select<Record1<Long>> getDeltaByDatamartAndDateSelect(DSLContext dsl, ActualDeltaRequest actualDeltaRequest) {
        SelectConditionStep<Record1<Long>> query = dsl.select(max(DELTA_DATA.SIN_ID))
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.equalIgnoreCase(actualDeltaRequest.getDatamart()))
                .and(DELTA_DATA.STATUS.eq(1));
        if (actualDeltaRequest.getDateTime() != null) {
            String adt = actualDeltaRequest.getDateTime().replaceAll("'", "");
            return query.and(DELTA_DATA.SYS_DATE.le(LocalDateTime.from(LOCAL_DATE_TIME.parse(adt))));
        }
        return query;
    }

    @Override
    public void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler) {
        log.debug("Получение {} дельт, начало", actualDeltaRequests.size());
        if (actualDeltaRequests.isEmpty()) {
            log.warn("Список запросов на дельты должен быть не пуст.");
            resultHandler.handle(Future.succeededFuture(Collections.emptyList()));
            return;
        }
        executor.query(dsl -> getUnionOfDeltaByDatamartAndDateSelects(dsl, actualDeltaRequests)).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Получение {} дельт, запрос выполнен", actualDeltaRequests.size());
                final List<Long> result = ar.result().stream()
                        .map(queryResult -> queryResult.get(0, Long.class))
                        .map(delta -> (delta == null) ? Long.valueOf(-1L) : delta)
                        .collect(Collectors.toList());
                log.debug("Получение {} дельт, результат: {}", actualDeltaRequests.size(), result);
                resultHandler.handle(Future.succeededFuture(result));
            } else {
                log.error("Получение {} дельт, ошибка: {}", actualDeltaRequests.size(), ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void getDeltaHotByDatamart(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler) {
        executor.query(dsl -> dsl.select(DELTA_DATA.LOAD_ID,
                DELTA_DATA.DATAMART_MNEMONICS,
                DELTA_DATA.SYS_DATE,
                DELTA_DATA.STATUS_DATE,
                DELTA_DATA.SIN_ID,
                DELTA_DATA.LOAD_PROC_ID,
                DELTA_DATA.STATUS)
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.eq(datamartMnemonic))
                .and(DELTA_DATA.LOAD_ID.in(dsl.select(max(DELTA_DATA.LOAD_ID)).from(DELTA_DATA).where(DELTA_DATA.DATAMART_MNEMONICS.eq(datamartMnemonic)))))
                .setHandler(ar -> {
                    initQueryDeltaResult(datamartMnemonic, resultHandler, ar);
                });
    }

    private void initQueryDeltaResult(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler, AsyncResult<QueryResult> ar) {
        if (ar.succeeded()) {
            final QueryResult result = ar.result();
            if (result.hasResults()) {
                DeltaRecord record = createDeltaRecord(result);
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                resultHandler.handle(Future.succeededFuture(null));
            }
        } else {
            log.error("Поиск дельты для витрины {}, ошибка {}", datamartMnemonic, ar.cause().getMessage());
            resultHandler.handle(Future.failedFuture(ar.cause()));
        }
    }

    @NotNull
    private DeltaRecord createDeltaRecord(QueryResult result) {
        return new DeltaRecord(
                result.get(0, Long.class),
                result.get(1, String.class),
                result.get(2, LocalDateTime.class),
                result.get(3, LocalDateTime.class),
                result.get(4, Long.class),
                result.get(5, String.class),
                DeltaLoadStatus.values()[result.get(6, Integer.class)]
        );
    }

    @Override
    public void getDeltaActualBySinIdAndDatamart(String datamartMnemonic, Long sinId, Handler<AsyncResult<DeltaRecord>> resultHandler) {
        executor.query(dsl -> dsl.select(
                DELTA_DATA.LOAD_ID,
                DELTA_DATA.DATAMART_MNEMONICS,
                DELTA_DATA.SYS_DATE,
                DELTA_DATA.STATUS_DATE,
                DELTA_DATA.SIN_ID,
                DELTA_DATA.LOAD_PROC_ID,
                DELTA_DATA.STATUS)
                .from(DELTA_DATA)
                .where(DELTA_DATA.DATAMART_MNEMONICS.eq(datamartMnemonic)
                        .and(DELTA_DATA.SIN_ID.eq(sinId))
                        .and(DELTA_DATA.STATUS.eq(DeltaLoadStatus.SUCCESS.ordinal()))))
                .setHandler(ar -> {
                    initQueryDeltaResult(datamartMnemonic, resultHandler, ar);
                });
    }

    @Override
    public void insertDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.insertInto(DELTA_DATA)
                .set(DELTA_DATA.DATAMART_MNEMONICS, delta.getDatamartMnemonic())
                .set(DELTA_DATA.SYS_DATE, delta.getSysDate())
                .set(DELTA_DATA.STATUS_DATE, delta.getStatusDate())
                .set(DELTA_DATA.SIN_ID, delta.getSinId())
                .set(DELTA_DATA.LOAD_PROC_ID, delta.getLoadProcId())
                .set(DELTA_DATA.STATUS, delta.getStatus().ordinal()))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void updateDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.update(DELTA_DATA)
                .set(DELTA_DATA.SYS_DATE, delta.getSysDate())
                .set(DELTA_DATA.STATUS_DATE, delta.getStatusDate())
                .set(DELTA_DATA.STATUS, delta.getStatus().ordinal())
                .where(DELTA_DATA.LOAD_ID.eq(delta.getLoadId())))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
