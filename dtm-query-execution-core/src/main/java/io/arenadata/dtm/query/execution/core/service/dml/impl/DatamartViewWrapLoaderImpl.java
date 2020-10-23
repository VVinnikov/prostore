package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.DatamartView;
import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewPair;
import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewWrap;
import io.arenadata.dtm.query.execution.core.service.dml.DatamartViewWrapLoader;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class DatamartViewWrapLoaderImpl implements DatamartViewWrapLoader {

    private final ServiceDbFacade serviceDbFacade;

    @SuppressWarnings("unchecked")
    @Override
    public Future<List<DatamartViewWrap>> loadViews(Set<DatamartViewPair> byLoadViews) {
        return Future.future(viewsPromise -> CompositeFuture.join(getLoaderFutures(groupByDatamart(byLoadViews)))
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    viewsPromise.complete(
                        ar.result().list().stream()
                            .map(it -> (List<DatamartViewWrap>) it)
                            .flatMap(List::stream)
                            .collect(toList())
                    );
                } else {
                    viewsPromise.fail(ar.cause());
                }
            }));
    }

    private Map<String, List<DatamartViewPair>> groupByDatamart(Set<DatamartViewPair> byLoadViews) {
        return byLoadViews.stream()
            .collect(Collectors.groupingBy(DatamartViewPair::getDatamart, toList()));
    }

    private List<Future> getLoaderFutures(Map<String, List<DatamartViewPair>> groupByDatamart) {
        return groupByDatamart
            .entrySet().stream()
            .map(e -> Future.future((Promise<List<DatamartViewWrap>> viewsByDatamartPromise) -> {
                val viewNames = getViewNames(e.getValue());
                val datamart = e.getKey();
                EntityDao entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
                CompositeFuture.join(
                    viewNames.stream()
                        .map(viewName -> entityDao.getEntity(datamart, viewName))
                        .collect(toList())
                ).onSuccess(cf -> {
                    val datamartViews = toDatamartViewWraps(datamart, cf.list());
                    viewsByDatamartPromise.complete(datamartViews);
                })
                    .onFailure(viewsByDatamartPromise::fail);
            }))
            .collect(toList());
    }

    private List<DatamartViewWrap> toDatamartViewWraps(String datamart, List<Entity> entities) {
        return entities.stream()
            .filter(entity -> EntityType.VIEW == entity.getEntityType())
            .map(entity -> new DatamartView(entity.getName(), entity.getViewQuery()))
            .map(v -> new DatamartViewWrap(datamart, v))
            .collect(toList());
    }

    private List<String> getViewNames(List<DatamartViewPair> viewPairs) {
        return viewPairs.stream()
            .map(DatamartViewPair::getViewName)
            .collect(toList());
    }
}
