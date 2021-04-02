package io.arenadata.dtm.query.execution.plugin.adg.calcite.model.schema;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import lombok.val;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.ArrayList;
import java.util.List;

public class AdgDtmTable extends DtmTable {
    public AdgDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity);
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
