package io.arenadata.dtm.query.execution.core.rollback.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EraseWriteOpResult;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackWriteOpsQueryResultFactory;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class RollbackWriteOpsQueryResultFactoryImpl implements RollbackWriteOpsQueryResultFactory {

    public static final String TABLE_NAME_COLUMN = "table_name";
    public static final String SYS_CN_OPS_COLUMN = "sys_cn_operations";

    @Override
    public QueryResult create(List<EraseWriteOpResult> eraseOps) {
        List<Map<String, Object>> eraseResult = eraseOps.stream()
                .collect(Collectors.groupingBy(EraseWriteOpResult::getTableName,
                        Collectors.mapping(er -> String.valueOf(er.getSysCn()), Collectors.joining(", "))))
                .entrySet().stream().map(er -> {
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put(TABLE_NAME_COLUMN, er.getKey());
                    resultMap.put(SYS_CN_OPS_COLUMN, er.getValue());
                    return resultMap;
                }).collect(Collectors.toList());
        return QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder()
                                .name(TABLE_NAME_COLUMN)
                                .type(ColumnType.VARCHAR)
                                .build(),
                        ColumnMetadata.builder()
                                .name(SYS_CN_OPS_COLUMN)
                                .type(ColumnType.VARCHAR)
                                .build()))
                .result(eraseResult)
                .build();
    }
}
