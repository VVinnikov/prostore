package ru.ibs.dtm.query.execution.core.service.dml.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;
import ru.ibs.dtm.query.execution.core.dto.dml.DatamartViewPair;
import ru.ibs.dtm.query.execution.core.dto.dml.DatamartViewWrap;
import ru.ibs.dtm.query.execution.core.service.dml.DatamartViewWrapLoader;

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

    @NotNull
    private List<Future> getLoaderFutures(Map<String, List<DatamartViewPair>> groupByDatamart) {
        return groupByDatamart
                .entrySet().stream()
                .map(e -> Future.future((Promise<List<DatamartViewWrap>> viewsByDatamartPromise) -> {
                    val viewNames = getViewNames(e.getValue());
                    val datamart = e.getKey();
                    serviceDbFacade.getServiceDbDao().getViewServiceDao().findViewsByDatamart(datamart, viewNames, ar -> {
                        if (ar.succeeded()) {
                            val datamartViews = toDatamartViewWraps(datamart, ar.result());
                            viewsByDatamartPromise.complete(datamartViews);
                        } else {
                            viewsByDatamartPromise.fail(ar.cause());
                        }
                    });
                }))
                .collect(toList());
    }

    @NotNull
    private List<DatamartViewWrap> toDatamartViewWraps(String datamart, List<DatamartView> datamartViews) {
        return datamartViews.stream()
                .map(v -> new DatamartViewWrap(datamart, v))
                .collect(toList());
    }

    @NotNull
    private List<String> getViewNames(List<DatamartViewPair> viewPairs) {
        return viewPairs.stream()
                .map(DatamartViewPair::getViewName)
                .collect(toList());
    }
}
