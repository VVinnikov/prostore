package ru.ibs.dtm.query.calcite.core.dtm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmRelation;

/**
 * Отдельный оператор для чтения таблицы
 */
public class DtmTableScan extends TableScan implements DtmRelation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DtmTableScan.class);

    private DtmConvention convention;
    private String tableName;

    public DtmTableScan(RelOptCluster cluster, RelOptTable table, DtmConvention convention, String tableName) {
        super(cluster, cluster.traitSetOf(convention), new ArrayList<RelHint>(), table);
        this.convention = convention;
        this.tableName = tableName;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DtmTableScan(super.getCluster(), super.getTable(), convention, tableName);
    }

    @Override
    public RelNode project(ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields, RelBuilder relBuilder) {
        if (fieldsUsed == ImmutableBitSet.range(rowType.getFieldCount()) && extraFields.isEmpty()) {
            return this;
        }
        RelNode relNode = copy(traitSet, getInputs());

        List<RelDataTypeField> currentFields = relNode.getRowType().getFieldList();
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(relBuilder.getTypeFactory());
        fieldsUsed.forEach(it -> builder.add(currentFields.get(it)));
        RelDataType newRowType = builder.build();
        try {
            Field rowTypeField = relNode.getClass().getField("rowType");
            rowTypeField.setAccessible(true);
            rowTypeField.set(relNode, newRowType);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.error("Ошибка установки параметра для relNode", e);
        }
        return relNode;
    }

    public String getTableName() {
        return tableName;
    }
}
