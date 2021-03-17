package io.arenadata.dtm.query.calcite.core.util;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class RelNodeUtil {

    public static boolean isNeedToTrimSortColumns(RelRoot relRoot, RelNode sourceRelNode) {
        return sourceRelNode instanceof LogicalSort
                && sourceRelNode.getRowType().getFieldCount() != relRoot.validatedRowType.getFieldCount();
    }

    public static RelNode trimUnusedSortColumn(RelBuilder relBuilder, RelNode relNode, RelDataType validatedRowType) {
        relBuilder.clear();
        relBuilder.push(relNode);
        ImmutableList<RexNode> fields = relBuilder.fields(validatedRowType.getFieldNames());
        return relBuilder.project(fields).build();
    }
}
