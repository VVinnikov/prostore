package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.query.execution.plugin.adb.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static ru.ibs.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;

@Service
@Slf4j
public class AdbCalciteDmlQueryExtendServiceImpl implements QueryExtendService {

    public static final String TABLE_PREFIX = "_";

    @Override
    public RelNode extendQuery(QueryGeneratorContext context) {
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
                if (!context.getDeltaIterator().hasNext()) {
                    throw new RuntimeException("No parameters defined to enrich the request");
                }
                relBuilder.push(insertModifiedTableScan(relBuilder, node, deltaIterator.next()));
            } else {
                relBuilder.push(node);
            }
            return relBuilder.build();
        }
        node.getInputs().forEach(input -> newInput.add(iterateTree(context, input)));
        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        return relBuilder.build();
    }

    RelNode insertModifiedTableScan(RelBuilder parentBuilder, RelNode tableScan, DeltaInformation deltaInfo) {
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
        RelNode topRelNode;
        RelNode bottomRelNode;

        initHistoryTableName(mutableQualifiedName, name);
        switch (deltaInfo.getType()) {
            case STARTED_IN:
                topRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                initActualTableName(mutableQualifiedName, name);
                bottomRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                break;
            case FINISHED_IN:
                topRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                initActualTableName(mutableQualifiedName, name);
                bottomRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                break;
            case DATETIME:
            case NUM:
                topRelNode = createTopRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                initActualTableName(mutableQualifiedName, name);
                bottomRelNode = createBottomRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                break;
            default:
                throw new RuntimeException(String.format("Incorrect delta type %s, expected values: %s!",
                        deltaInfo.getType(), DeltaType.values()));
        }
        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    }

    private void initHistoryTableName(ArrayList<String> mutableQualifiedName, StringBuilder name) {
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + TABLE_PREFIX + HISTORY_TABLE);
    }

    private void initActualTableName(ArrayList<String> mutableQualifiedName, StringBuilder name) {
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + TABLE_PREFIX + ACTUAL_TABLE);
    }

    private RelNode createRelNodeDeltaStartedIn(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                List<RexNode> rexNodes, List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_ATTR),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaFrom())),
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_ATTR),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaTo()))
                )
        ).project(rexNodes).build();
    }

    private RelNode createRelNodeDeltaFinishedIn(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                 List<RexNode> rexNodes, List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_ATTR),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaFrom() - 1)),
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_ATTR),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaTo() - 1)),
                        relBuilder.call(SqlStdOperatorTable.EQUALS,
                                relBuilder.field(SYS_OP_ATTR),
                                relBuilder.literal(1))
                )
        ).project(rexNodes).build();
    }

    private RelNode createTopRelNodeDeltaNum(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                             List<RexNode> rexNodes, List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_ATTR),
                                relBuilder.literal(deltaInfo.getSelectOnNum())),
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_ATTR),
                                relBuilder.literal(deltaInfo.getSelectOnNum()))
                )
        ).project(rexNodes).build();
    }

    private RelNode createBottomRelNodeDeltaNum(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                List<RexNode> rexNodes, List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_ATTR),
                        relBuilder.literal(deltaInfo.getSelectOnNum()))).project(rexNodes).build();
    }

}
