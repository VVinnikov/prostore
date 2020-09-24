package ru.ibs.dtm.query.calcite.core.schema;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import ru.ibs.dtm.query.calcite.core.util.CalciteUtil;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

import java.util.ArrayList;

public abstract class DtmTable extends AbstractQueryableTable implements TranslatableTable {

    protected final QueryableSchema dtmSchema;
    protected final DatamartTable datamartTable;

    public DtmTable(QueryableSchema dtmSchema, DatamartTable datamartTable) {
        super(Object[].class);
        this.dtmSchema = dtmSchema;
        this.datamartTable = datamartTable;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        //TODO: complete the task of executing the request
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        datamartTable.getTableAttributes()
                .forEach(it -> builder.add(
                        it.getMnemonic(),
                        CalciteUtil.valueOf(it.getType().getValue())
                        //FIXME implement setting the precision and scale attributes from length and accuracy
                        //it.getLength(),
                        //it.getAccuracy()
                ).nullable(true));
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable, new ArrayList<>());
    }
}
