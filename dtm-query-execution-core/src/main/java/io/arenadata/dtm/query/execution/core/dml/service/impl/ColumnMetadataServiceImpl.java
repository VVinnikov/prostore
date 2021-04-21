package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ColumnMetadataServiceImpl implements ColumnMetadataService {
    private final QueryParserService parserService;

    public ColumnMetadataServiceImpl(@Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.parserService = parserService;
    }

    @Override
    public Future<List<ColumnMetadata>> getColumnMetadata(QueryParserRequest request) {
        return parserService.parse(request)
                .map(response -> getColumnMetadataInner(response.getRelNode()));
    }

    @Override
    public Future<List<ColumnMetadata>> getColumnMetadata(RelRoot relNode) {
        return Future.succeededFuture(getColumnMetadataInner(relNode));
    }

    private List<ColumnMetadata> getColumnMetadataInner(RelRoot relNode) {
        return relNode.project().getRowType().getFieldList().stream()
                .sorted(Comparator.comparing(RelDataTypeField::getIndex))
                .map(f -> new ColumnMetadata(f.getName(), getType(f.getType()), getSize(f)))
                .collect(Collectors.toList());
    }

    private ColumnType getType(RelDataType type) {
        return CalciteUtil.toColumnType(type.getSqlTypeName());
    }

    private Integer getSize(RelDataTypeField field) {
        ColumnType type = getType(field.getType());
        switch (type) {
            case VARCHAR:
            case CHAR:
            case UUID:
            case TIME:
            case TIMESTAMP:
                return field.getValue().getPrecision();
            default:
                return null;
        }
    }
}