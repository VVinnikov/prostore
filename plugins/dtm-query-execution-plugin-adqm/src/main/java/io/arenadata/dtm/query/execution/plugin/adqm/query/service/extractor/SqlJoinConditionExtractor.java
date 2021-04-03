package io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor;

import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmJoinQuery;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public interface SqlJoinConditionExtractor {

    List<AdqmJoinQuery> extract(RelNode relNode);
}
