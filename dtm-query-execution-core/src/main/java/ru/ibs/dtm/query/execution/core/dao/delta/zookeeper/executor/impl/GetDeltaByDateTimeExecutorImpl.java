package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaByDateTimeExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotExistException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotFoundException;
import ru.ibs.dtm.query.execution.core.dto.delta.OkDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class GetDeltaByDateTimeExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaByDateTimeExecutor {

    public GetDeltaByDateTimeExecutorImpl(ZookeeperExecutor executor,
                                             @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart, LocalDateTime dateTime) {
        val ctx = new DeltaContext();
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                ctx.setDelta(delta);
                val deltaDateTime = delta.getOk().getDeltaDate();
                return deltaDateTime.isBefore(dateTime) || deltaDateTime.isEqual(dateTime);
            })
            .compose(isDeltaOk -> isDeltaOk ?
                Future.succeededFuture(ctx.getDelta().getOk())
                : findByDays(datamart, dateTime))
            .onSuccess(r -> {
                log.debug("get delta ok by datamart[{}], dateTime[{}] completed successfully: [{}]", datamart, dateTime, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("can't get delta ok on datamart[%s], dateTime[%s]",
                    datamart,
                    dateTime);
                log.error(errMsg, error);
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.fail(new DatamartNotExistsException(datamart));
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Future<OkDelta> findByDays(String datamart, LocalDateTime dateTime) {
        val date = dateTime.toLocalDate();
        Promise<OkDelta> resultPromise = Promise.promise();
        getDatamartDeltaDays(datamart, date)
            .onSuccess(days -> {
                if (days.size() > 0) {
                    val dayIterator = days.iterator();
                    findByDay(datamart, dayIterator, resultPromise);
                } else {
                    resultPromise.fail(new DeltaNotFoundException());
                }
            })
            .onFailure(resultPromise::fail);
        return resultPromise.future();
    }

    private Future<List<LocalDate>> getDatamartDeltaDays(String datamart, LocalDate date) {
        return executor.getChildren(getDeltaPath(datamart) + "/date")
            .map(daysStr -> daysStr.stream()
                .map(LocalDate::parse)
                .filter(day -> date.isAfter(day) || date.isEqual(day))
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList()));
    }

    private void findByDay(String datamart,
                           Iterator<LocalDate> dayIterator,
                           Promise<OkDelta> resultPromise) {
        val day = dayIterator.next();
        getDeltaOkByMaxDeltaDateTime(datamart, day)
            .onSuccess(okDelta -> {
                if (okDelta != null) {
                    resultPromise.complete(okDelta);
                } else if (dayIterator.hasNext()) {
                    findByDay(datamart, dayIterator, resultPromise);
                } else {
                    resultPromise.fail(new DeltaNotExistException());
                }
            })
            .onFailure(resultPromise::fail);
    }

    private Future<OkDelta> getDeltaOkByMaxDeltaDateTime(String datamart, LocalDate day) {
        return executor.getChildren(getDeltaDatePath(datamart, day))
            .map(times -> times.stream()
                .map(LocalTime::parse)
                .max(Comparator.naturalOrder()))
            .compose(timeOpt -> timeOpt
                .map(localTime -> {
                    val dateTime = LocalDateTime.of(day, localTime);
                    return executor.getData(getDeltaDateTimePath(datamart, dateTime));
                }).orElse(Future.succeededFuture()))
            .map(this::getOkDelta);
    }

    private OkDelta getOkDelta(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            return deserializedOkDelta(bytes);
        }
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaByDateTimeExecutor.class;
    }
}
