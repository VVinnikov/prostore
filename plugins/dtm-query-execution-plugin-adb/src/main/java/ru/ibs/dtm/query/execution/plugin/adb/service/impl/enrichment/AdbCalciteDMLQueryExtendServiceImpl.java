package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import java.util.ArrayList;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adb.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;

import static ru.ibs.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;

@Service
@Slf4j
public class AdbCalciteDMLQueryExtendServiceImpl implements QueryExtendService {
    @Override
    public RelNode extendQuery(QueryGeneratorContext context) {
        if (!context.getDeltaIterator().hasNext()) {
            throw new RuntimeException("Не определены параметры для обогащения запроса");
        }
        context.getRelBuilder().clear();
        val relNode = iterateTree(context, context.getRelNode().rel);
        context.getRelBuilder().clear();
        return relNode;
    }

    RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        val deltaIterator = context.getDeltaIterator();
        val relBuilder = context.getRelBuilder();
        val newInput = new ArrayList<RelNode>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                relBuilder.push(insertModifiedTableScan(relBuilder, node, deltaIterator.next().getDeltaNum()));
            }
            return relBuilder.build();
        }
        node.getInputs().forEach(input -> newInput.add(iterateTree(context, input)));
        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        return relBuilder.build();
    }

    RelNode insertModifiedTableScan(RelBuilder parentBuilder, RelNode tableScan, Long selectOnDelta) {
        val relBuilder = RelBuilder
                .proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), parentBuilder.getRelOptSchema());
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val mutableQualifiedName = new ArrayList<String>(qualifiedName);

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        val rexNodes = new ArrayList<RexNode>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
        );
        val name = new StringBuilder(mutableQualifiedName.get(mutableQualifiedName.size() - 1));
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + "_" + HISTORY_TABLE);
        val topRelNode = relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_ATTR),
                                relBuilder.literal(selectOnDelta)),
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_ATTR),
                                relBuilder.literal(selectOnDelta))
                )
        ).project(rexNodes).build();

        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + "_" + ACTUAL_TABLE);
        val bottomRelNode = relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_ATTR),
                        relBuilder.literal(selectOnDelta))).project(rexNodes).build();

        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();

    }

}
