package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


@Slf4j
@Service("adgCalciteDmlQueryExtendService")
public class AdgCalciteDmlQueryExtendServiceImpl implements QueryExtendService {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgCalciteDmlQueryExtendServiceImpl(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        RelNode relNode = iterateTree(context, context.getRelNode().rel);
        context.getRelBuilder().clear();
        return relNode;
    }

    RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        List<RelNode> newInput = new ArrayList<>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                context.getRelBuilder().push(insertModifiedTableScan(context, node, context.getDeltaIterator().next()));
            }
            return context.getRelBuilder().build();
        }
        node.getInputs().forEach(input -> {
            newInput.add(iterateTree(context, input));
        });
        context.getRelBuilder().push(node.copy(node.getTraitSet(), newInput));
        return context.getRelBuilder().build();
    }

    RelNode insertModifiedTableScan(QueryGeneratorContext context, RelNode tableScan, DeltaInformation deltaInfo) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(),
                        ((CalciteCatalogReader) context.getRelBuilder().getRelOptSchema())
                                .withSchemaPath(Collections.singletonList(deltaInfo.getSchemaName())));

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        List<RexNode> rexNodes = new ArrayList<>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
        );

        val qualifiedName = tableScan.getTable().getQualifiedName();
        val tableName = qualifiedName.get(qualifiedName.size() > 1 ? 1 : 0);
        val schemaName = deltaInfo.getSchemaName();
        val queryRequest = context.getQueryRequest();
        val tableNames = helperTableNamesFactory.create(queryRequest.getSystemName(),
                schemaName, tableName);
        RelNode topRelNode;
        RelNode bottomRelNode;

        switch (deltaInfo.getType()) {
            case STARTED_IN:
                topRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                bottomRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, tableNames.getActual());
                break;
            case FINISHED_IN:
                topRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                bottomRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, tableNames.getActual());
                break;
            case NUM:
                topRelNode = createTopRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                bottomRelNode = createBottomRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, tableNames.getActual());
                break;
            default:
                throw new RuntimeException(String.format("Incorrect delta type %s, expected values: %s!",
                        deltaInfo.getType(), DeltaType.values()));
        }

        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    }

    private RelNode createRelNodeDeltaStartedIn(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                List<RexNode> rexNodes, String tableName) {
        return relBuilder.scan(tableName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaFrom())),
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaTo()))
                )
        ).project(rexNodes).build();
    }

    private RelNode createRelNodeDeltaFinishedIn(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                 List<RexNode> rexNodes, String tableName) {
        return relBuilder.scan(tableName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaFrom() - 1)),
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaInterval().getDeltaTo() - 1)),
                        relBuilder.call(SqlStdOperatorTable.EQUALS,
                                relBuilder.field(SYS_OP_FIELD),
                                relBuilder.literal(1))
                )
        ).project(rexNodes).build();
    }

    private RelNode createTopRelNodeDeltaNum(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                             List<RexNode> rexNodes, String tableName) {
        return relBuilder.scan(tableName).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaNum())),
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_FIELD),
                                relBuilder.literal(deltaInfo.getDeltaNum()))
                )
        ).project(rexNodes).build();
    }

    private RelNode createBottomRelNodeDeltaNum(DeltaInformation deltaInfo, RelBuilder relBuilder,
                                                List<RexNode> rexNodes, String tableName) {
        return relBuilder.scan(tableName).filter(
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_FIELD),
                        relBuilder.literal(deltaInfo.getDeltaNum()))).project(rexNodes).build();
    }

}
