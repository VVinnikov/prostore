package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.CheckColumnTypesService;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class CheckColumnTypesServiceImpl implements CheckColumnTypesService {
    private final QueryParserService queryParserService;

    @Autowired
    public CheckColumnTypesServiceImpl(@Qualifier("coreCalciteDMLQueryParserService")
                                               QueryParserService coreCalciteDMLQueryParserService) {
        this.queryParserService = coreCalciteDMLQueryParserService;
    }

    @Override
    public Future<Boolean> check(List<ColumnType> checkColumnTypes, QueryParserRequest queryParseRequest) {
        return Future.future(promise -> queryParserService.parse(queryParseRequest, ar -> {
            try {
                if (ar.succeeded()) {
                    RelNode relNode = ar.result().getRelNode().project();
                    List<ColumnType> columns = relNode.getRowType().getFieldList().stream()
                            .map(field -> ColumnType.valueOf(field.getType().getSqlTypeName().getName()))
                            .collect(Collectors.toList());
                    promise.complete(columns.equals(checkColumnTypes));
                } else {
                    promise.fail(ar.cause());

                }
            } catch (Exception e) {
                promise.fail(e);
            }
        }));
    }
}
