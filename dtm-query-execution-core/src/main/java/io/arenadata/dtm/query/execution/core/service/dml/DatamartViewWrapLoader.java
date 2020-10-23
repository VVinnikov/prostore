package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewPair;
import io.arenadata.dtm.query.execution.core.dto.dml.DatamartViewWrap;
import io.vertx.core.Future;

import java.util.List;
import java.util.Set;

public interface DatamartViewWrapLoader {
    Future<List<DatamartViewWrap>> loadViews(Set<DatamartViewPair> byLoadViews);
}
