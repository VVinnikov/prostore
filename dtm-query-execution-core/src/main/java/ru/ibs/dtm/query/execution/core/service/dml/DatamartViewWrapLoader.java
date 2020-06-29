package ru.ibs.dtm.query.execution.core.service.dml;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.core.dto.dml.DatamartViewPair;
import ru.ibs.dtm.query.execution.core.dto.dml.DatamartViewWrap;

import java.util.List;
import java.util.Set;

public interface DatamartViewWrapLoader {
    Future<List<DatamartViewWrap>> loadViews(Set<DatamartViewPair> byLoadViews);
}
