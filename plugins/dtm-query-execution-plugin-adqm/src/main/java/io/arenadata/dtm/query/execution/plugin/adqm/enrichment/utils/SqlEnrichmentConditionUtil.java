package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.utils;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import lombok.val;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;

import static io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants.*;

public class SqlEnrichmentConditionUtil {
    private static final int ONE_LITERAL = 1;
    private static final int LIMIT_1 = 1;

    private SqlEnrichmentConditionUtil() {
    }

    public static List<RexNode> createDeltaConditions(RelBuilder relBuilder, DeltaInformation deltaInfo) {
        switch (deltaInfo.getType()) {
            case STARTED_IN:
                return createRelNodeDeltaStartedIn(relBuilder, deltaInfo);
            case FINISHED_IN:
                return createRelNodeDeltaFinishedIn(relBuilder, deltaInfo);
            case DATETIME:
            case WITHOUT_SNAPSHOT:
            case NUM:
                return createRelNodeDeltaNum(relBuilder, deltaInfo);
            default:
                throw new DataSourceException(String.format("Incorrect delta type %s, expected values: %s!",
                        deltaInfo.getType(),
                        Arrays.toString(DeltaType.values())));
        }
    }

    private static List<RexNode> createRelNodeDeltaStartedIn(RelBuilder relBuilder, DeltaInformation deltaInfo) {
        return Arrays.asList(
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom())),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo()))
        );
    }

    private static List<RexNode> createRelNodeDeltaFinishedIn(RelBuilder relBuilder, DeltaInformation deltaInfo) {
        return Arrays.asList(
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.field(SYS_TO_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom() - 1)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_TO_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo() - 1)),
                relBuilder.call(SqlStdOperatorTable.EQUALS,
                        relBuilder.field(SYS_OP_FIELD),
                        relBuilder.literal(1))
        );
    }

    private static List<RexNode> createRelNodeDeltaNum(RelBuilder relBuilder, DeltaInformation deltaInfo) {
        return Arrays.asList(
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnNum())),
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.field(SYS_TO_FIELD),
                        relBuilder.literal(deltaInfo.getSelectOnNum()))
        );
    }

    public static SqlBinaryOperator getSignOperatorCondition(boolean isTop) {
        return isTop ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
    }

    public static RexNode createSignSubQuery(TableScan tableScan, boolean isTop) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), tableScan.getTable().getRelOptSchema());
        val node = relBuilder.scan(tableScan.getTable().getQualifiedName())
                .filter(relBuilder.call(SqlStdOperatorTable.LESS_THAN,
                        relBuilder.field(SIGN_FIELD),
                        relBuilder.literal(0)))
                .project(relBuilder.alias(relBuilder.literal(ONE_LITERAL), "r"))
                .limit(0, LIMIT_1)
                .build();
        return relBuilder.call(isTop ?
                SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL, RexSubQuery.scalar(node));
    }
}
