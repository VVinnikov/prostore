package ru.ibs.dtm.query.execution.plugin.adg.calcite.schema;

import java.util.ArrayList;
import java.util.List;
import lombok.val;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.DatamartClass;

public class AdgDtmTable extends DtmTable {
    public AdgDtmTable(QueryableSchema dtmSchema, DatamartClass datamartClass) {
        super(dtmSchema, datamartClass);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), RelOptTableImpl.create(
                relOptTable.getRelOptSchema(),
                relOptTable.getRowType(),
                getTableNameWithoutSchema(relOptTable),
                relOptTable.getExpression(AdgDtmTable.class)
        ), new ArrayList<>());
    }

    private List<String> getTableNameWithoutSchema(RelOptTable relOptTable) {
        val qualifiedName = relOptTable.getQualifiedName();
        val size = qualifiedName.size();
        return size == 1 ? qualifiedName : qualifiedName.subList(size - 1, size);
    }
}
