package ru.ibs.dtm.query.calcite.core.schema;

import java.util.ArrayList;
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
        //TODO: доделать в задаче исполнения запроса
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        datamartTable.getTableAttributes()
                .forEach(it -> builder.add(
                        it.getMnemonic(),
                        CalciteUtil.valueOf(it.getType().getValue())
                ).nullable(true));
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {

//    RelTraitSet traitSet = RelTraitSet.createEmpty();
//    traitSet.plus(dtmSchema.getConvention());
        //Создаем реляционный оператор сканирования таблицы
        //return new DtmTableScan(context.getCluster(), relOptTable, dtmSchema.getConvention(), datamartClass.getMnemonic());
        //На текущий этап сделано через LogicalTableScan
        return LogicalTableScan.create(context.getCluster(), relOptTable, new ArrayList());
    }
}
