package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.extractor;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.query.AdqmJoinQuery;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public interface SqlJoinConditionExtractor {

    List<AdqmJoinQuery> extract(RelNode relNode);
}
