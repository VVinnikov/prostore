package io.arenadata.dtm.query.execution.plugin.adqm.calcite.schema;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class AdqmDtmTable extends DtmTable {
    public AdqmDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        entity.getFields().forEach(it -> {
                if (it.getSize() != null && it.getAccuracy() != null) {
                    builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getSize(), it.getAccuracy())
                        .nullable(isNullable(it));
                } else if (it.getSize() != null) {
                    builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getSize())
                        .nullable(isNullable(it));
                } else {
                    if (it.getType() == ColumnType.UUID) {
                        builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), UUID_SIZE)
                            .nullable(isNullable(it));
                    } else if ((it.getType() == ColumnType.TIME || it.getType() == ColumnType.TIMESTAMP)
                        && it.getAccuracy() != null) {
                        builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getAccuracy())
                            .nullable(isNullable(it));
                    } else {
                        builder.add(it.getName(), CalciteUtil.valueOf(it.getType()))
                            .nullable(isNullable(it));
                    }
                }
            }
        );
        return builder.build();
    }

    //ToDo add support isNullable
    private boolean isNullable(EntityField it) {
        return true;
    }

}
