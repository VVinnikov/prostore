package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryExtendService;

import java.util.*;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.*;


@Slf4j
@Service("adqmCalciteDmlQueryExtendService")
public class AdqmCalciteDmlQueryExtendServiceImpl implements QueryExtendService {
    private static final String ONE_LITERAL = "1";
    private static final int BY_ONE_TABLE = 0;
    private static final int ONE_TABLE = 1;
    private static final int LIMIT_1 = 1;
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    public AdqmCalciteDmlQueryExtendServiceImpl(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    /**
     * Converts a relational expression to SQL in a given dialect.
     */
    private static String toSql(RelNode root, SqlDialect dialect) {
        final RelToSqlConverter converter = new RelToSqlConverter(dialect);
        final SqlNode sqlNode = converter.visitChild(0, root).asStatement();
        return sqlNode.toSqlString(dialect).getSql();
    }

    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        RelNode replacingTablesNode = iterateReplacingTableName(context, context.getRelNode().rel, false);
        RelNode enrichmentNode = iterateEnrichProject(
            DeltaIteratorContext.builder()
                .bottomIterator(context.getQueryRequest().getDeltaInformations().iterator())
                .topIterator(context.getQueryRequest().getDeltaInformations().iterator())
                .generatorContext(context)
                .build(),
            replacingTablesNode
        );
        context.getRelBuilder().clear();
        return enrichmentNode;
    }

    private RelNode iterateEnrichProject(DeltaIteratorContext deltaIteratorContext, RelNode node) {
        List<RelNode> newInputs = new ArrayList<>();
        node.getInputs().forEach(input -> newInputs.add(iterateEnrichProject(deltaIteratorContext, input)));
        val preparedInnerNodes = node.copy(node.getTraitSet(), newInputs);
        if (preparedInnerNodes instanceof Project) {
            val project = (Project) preparedInnerNodes;
            var enrichmentTopProject = getEnrichmentTopProject(deltaIteratorContext, project);
            val enrichmentBottomProject = getEnrichmentBottomProject(deltaIteratorContext, project);
            return deltaIteratorContext.getGeneratorContext()
                .getRelBuilder()
                .push(enrichmentTopProject)
                .push(enrichmentBottomProject)
                .union(true).build();
        }
        return preparedInnerNodes;
    }

    private RelNode getEnrichmentTopProject(DeltaIteratorContext deltaIteratorContext, Project project) {
        return enrichProject(
            project,
            deltaIteratorContext,
            deltaIteratorContext.getTopIterator(),
            true);
    }

    private RelNode getEnrichmentBottomProject(DeltaIteratorContext deltaIteratorContext, Project project) {
        return enrichProject(
            project,
            deltaIteratorContext,
            deltaIteratorContext.getBottomIterator(),
            false);
    }

    private RelNode enrichProject(Project project,
                                  DeltaIteratorContext deltaIteratorContext,
                                  Iterator<DeltaInformation> deltaIterator,
                                  boolean isTop) {
        List<TableScan> tables = project.getInputs().stream()
            .flatMap(input -> findTables(input).stream())
            .collect(Collectors.toList());
        if (tables.isEmpty()) {
            return project;
        }
        var projectInput = project.getInput();
        val schemaPaths = deltaIteratorContext.getGeneratorContext()
            .getQueryRequest()
            .getDeltaInformations().stream()
            .map(DeltaInformation::getSchemaName)
            .distinct()
            .collect(Collectors.toList());
        val builder = RelBuilder.proto(projectInput.getCluster().getPlanner().getContext())
            .create(projectInput.getCluster(),
                ((CalciteCatalogReader) deltaIteratorContext.getGeneratorContext().getRelBuilder().getRelOptSchema())
                    .withSchemaPath(schemaPaths));

        RelNode enrichmentProject;
        if (projectInput instanceof Filter) {
            val firstCondition = ((Filter) projectInput).getCondition();
            tables.forEach(tableScan -> builder.scan(tableScan.getTable().getQualifiedName()));
            val allDeltaCondition = new ArrayList<>(getDeltaConditions(deltaIterator, tables, builder));
            allDeltaCondition.add(firstCondition);
            Filter deltaFilter = ((Filter) projectInput)
                .copy(projectInput.getTraitSet(),
                    ((Filter) projectInput).getInput(),
                    builder.call(SqlStdOperatorTable.AND, allDeltaCondition));
            enrichmentProject = project.copy(
                project.getTraitSet(),
                deltaFilter,
                project.getProjects(),
                project.getRowType()
            );
        } else {
            tables.forEach(tableScan -> builder.scan(tableScan.getTable().getQualifiedName()));
            val allDeltaCondition = new ArrayList<>(getDeltaConditions(deltaIterator, tables, builder));
            val enrichmentNodes = projectInput.getInputs().stream()
                .map(relNode -> iterateEnrichProject(deltaIteratorContext, relNode))
                .collect(Collectors.toList());
            enrichmentProject = builder.push(projectInput.copy(projectInput.getTraitSet(), enrichmentNodes))
                .filter(builder.call(SqlStdOperatorTable.AND, allDeltaCondition))
                .project(project.getChildExps())
                .build();
        }
        val signConditions = tables.stream()
            .map(tableScan -> createSignSubQuery(builder, tableScan, isTop))
            .collect(Collectors.toList());

        return builder.push(enrichmentProject)
            .filter(signConditions.size() == ONE_TABLE ?
                signConditions.get(BY_ONE_TABLE) : builder.call(getSignOperatorCondition(isTop), signConditions))
            .build();
    }

    private SqlBinaryOperator getSignOperatorCondition(boolean isTop) {
        return isTop ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR;
    }

    private RexNode createSignSubQuery(RelBuilder builder,
                                       TableScan tableScan,
                                       boolean isTop) {
        RelNode node = builder.scan(tableScan.getTable().getQualifiedName())
            .filter(builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field(SIGN_FIELD),
                builder.literal(0)))
            .project(builder.alias(builder.literal(1), "r"))
            .limit(0, LIMIT_1)
            .build();
        return builder.call(isTop ?
            SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL, RexSubQuery.scalar(node));
    }

    private List<RexNode> getDeltaConditions(Iterator<DeltaInformation> deltaIterator,
                                             List<TableScan> tables,
                                             RelBuilder builder) {
        return tables.stream()
            .flatMap(tableScan -> {
                val deltaInfo = deltaIterator.next();

                val conditionContext = DeltaConditionContext.builder()
                    .deltaInfo(deltaInfo)
                    .tableCount(tables.size())
                    .tableScan(tableScan)
                    .builder(builder)
                    .finalize(false)
                    .build();

                switch (deltaInfo.getType()) {
                    case STARTED_IN:
                        return createRelNodeDeltaStartedIn(conditionContext).stream();
                    case FINISHED_IN:
                        return createRelNodeDeltaFinishedIn(conditionContext).stream();
                    case NUM:
                        return createRelNodeDeltaNum(conditionContext).stream();
                    default:
                        throw new RuntimeException(String.format("Incorrect delta type %s, expected values: %s!",
                            deltaInfo.getType(), Arrays.toString(DeltaType.values())));
                }
            }).collect(Collectors.toList());
    }

    private List<TableScan> findTables(RelNode node) {
        if (node instanceof TableScan) {
            return Collections.singletonList((TableScan) node);
        } else if (node instanceof Filter || node instanceof Join) {
            List<TableScan> tables = new ArrayList<>();
            node.getInputs().stream()
                .flatMap(n -> findTables(n).stream())
                .forEach(tables::add);
            return tables;
        } else {
            return Collections.emptyList();
        }
    }

    RelNode iterateReplacingTableName(QueryGeneratorContext context, RelNode node, boolean isShard) {
        List<RelNode> newInput = new ArrayList<>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                context.getRelBuilder().push(insertRenameTableScan(context, node, isShard));
            }
            return context.getRelBuilder().build();
        }
        for (int i = 0; i < node.getInputs().size(); i++) {
            RelNode input = node.getInput(i);
            newInput.add(iterateReplacingTableName(context, input, isShard(node, input, i)));
        }
        context.getRelBuilder().push(node.copy(node.getTraitSet(), newInput));
        return context.getRelBuilder().build();
    }

    private boolean isShard(RelNode parentNode, RelNode node, int i) {
        return node instanceof TableScan && parentNode instanceof Join && i > 0;
    }


    RelNode insertRenameTableScan(QueryGeneratorContext context,
                                  RelNode tableScan,
                                  boolean isShard) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
            .create(tableScan.getCluster(),
                ((CalciteCatalogReader) context.getRelBuilder().getRelOptSchema())
                    .withSchemaPath(context.getQueryRequest()
                        .getDeltaInformations().stream()
                        .map(DeltaInformation::getSchemaName)
                        .distinct()
                        .collect(Collectors.toList())));
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val queryRequest = context.getQueryRequest();
        val tableNames = helperTableNamesFactory.create(queryRequest.getSystemName(), qualifiedName.get(0), qualifiedName.get(1));
        RelNode table = relBuilder.scan(isShard ? tableNames.toQualifiedActualShard() : tableNames.toQualifiedActual())
            .build();
        return table;
    }

    private List<RexNode> createRelNodeDeltaStartedIn(DeltaConditionContext ctx) {
        val alias = ctx.getTableScan().getTable().getQualifiedName().get(1);
        val inputCount = ctx.getTableCount();
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaInterval().getDeltaFrom())),
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaInterval().getDeltaTo()))
        );
    }

    private List<RexNode> createRelNodeDeltaFinishedIn(DeltaConditionContext ctx) {
        val alias = ctx.getTableScan().getTable().getQualifiedName().get(1);
        val inputCount = ctx.getTableCount();
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaInterval().getDeltaFrom() - 1)),
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaInterval().getDeltaTo() - 1)),
            ctx.builder.call(SqlStdOperatorTable.EQUALS,
                ctx.builder.field(inputCount, alias, SYS_OP_FIELD),
                ctx.builder.literal(1))
        );
    }

    private List<RexNode> createRelNodeDeltaNum(DeltaConditionContext ctx) {
        val alias = ctx.getTableScan().getTable().getQualifiedName().get(1);
        val inputCount = ctx.getTableCount();
        return Arrays.asList(
            ctx.builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_FROM_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaNum())),
            ctx.builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                ctx.builder.field(inputCount, alias, SYS_TO_FIELD),
                ctx.builder.literal(ctx.deltaInfo.getDeltaNum()))
        );
    }

    @Data
    @Builder
    private static final class DeltaConditionContext {
        private DeltaInformation deltaInfo;
        private TableScan tableScan;
        private RelBuilder builder;
        private boolean finalize;
        private int tableCount;
    }

    @Data
    @Builder
    private static final class DeltaIteratorContext {
        private Iterator<DeltaInformation> bottomIterator;
        private Iterator<DeltaInformation> topIterator;
        private QueryGeneratorContext generatorContext;
    }

}
