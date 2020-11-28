package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.service.CheckColumnTypesService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class CheckColumnTypesServiceImpl implements CheckColumnTypesService {

    public static final String FAIL_CHECK_COLUMNS_PATTERN = "The types of columns of the destination table [%s] " +
            "and the types of the selection columns does not match!";
    private final QueryParserService queryParserService;

    @Autowired
    public CheckColumnTypesServiceImpl(@Qualifier("coreCalciteDMLQueryParserService")
                                               QueryParserService coreCalciteDMLQueryParserService) {
        this.queryParserService = coreCalciteDMLQueryParserService;
    }

    @Override
    public Future<Boolean> check(List<EntityField> destinationFields, QueryParserRequest queryParseRequest) {
        return Future.future(promise -> queryParserService.parse(queryParseRequest, ar -> {
            try {
                if (ar.succeeded()) {
                    val destinationColumns = destinationFields.stream()
                            .map(field -> CalciteUtil.valueOf(field.getType()))
                            .collect(Collectors.toList());
                    val sourceColumns = ar.result().getRelNode().validatedRowType.getFieldList().stream()
                            .map(field -> field.getType().getSqlTypeName())
                            .collect(Collectors.toList());
                    promise.complete(destinationColumns.equals(sourceColumns));
                } else {
                    promise.fail(ar.cause());

                }
            } catch (Exception e) {
                promise.fail(e);
            }
        }));
    }
}
