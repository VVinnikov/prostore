package io.arenadata.dtm.query.execution.core.service.query.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.service.query.CheckColumnTypesService;
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
        return queryParserService.parse(queryParseRequest)
                .map(response -> {
                    val destinationColumns = destinationFields.stream()
                            .map(field -> CalciteUtil.valueOf(field.getType()))
                            .collect(Collectors.toList());
                    val sourceColumns =
                            response.getRelNode().validatedRowType.getFieldList().stream()
                                    .map(field -> field.getType().getSqlTypeName())
                                    .collect(Collectors.toList());
                    return destinationColumns.equals(sourceColumns);
                });
    }
}