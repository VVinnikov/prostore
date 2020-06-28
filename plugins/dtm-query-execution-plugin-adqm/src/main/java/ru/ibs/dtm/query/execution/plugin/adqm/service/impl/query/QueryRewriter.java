package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.QueryEnrichmentProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class QueryRewriter {

    private final CalciteContextProvider calciteContextProvider;
    private final QueryEnrichmentProperties queryEnrichmentProperties;

    private final static String SUBQUERY_TEMPLATE = "select 1 from %s where sign < 0 limit 1";
    private final static String SUBQUERY_NOT_NULL_CHECK = "select 1 from dual where (select 1 from dual) is not null";
    private final static String SUBQUERY_NULL_CHECK = "select 1 from dual where (select 1 from dual) is null";
    private final static Pattern FINAL_PATTERN = Pattern.compile("`(\\w+)_final`", Pattern.CASE_INSENSITIVE);
    private final static Pattern TABLE_ALIAS_PATTERN = Pattern.compile("^\\s+(AS\\s+`\\w+`)");
    private final static String UNION_ALL_TEMPLATE = "select * from (select 1 from dual) union all select * from (select 1 from dual)";

    public QueryRewriter(CalciteContextProvider calciteContextProvider, QueryEnrichmentProperties queryEnrichmentProperties) {
        this.calciteContextProvider = calciteContextProvider;
        this.queryEnrichmentProperties = queryEnrichmentProperties;
    }

    public void rewrite(String sql, List<DeltaInformation> deltas, Handler<AsyncResult<String>> handler) {
        CalciteContext context = calciteContextProvider.context(null);
        try {
            SqlNode root = context.getPlanner().parse(sql);
            rewriteInternal(root, deltas, handler);
        } catch (SqlParseException e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private void rewriteInternal(SqlNode root, List<DeltaInformation> deltas, Handler<AsyncResult<String>> handler) {
        // 1. Modify query - add filter for sys_from/sys_to columns based on deltas
        SqlNode withDeltaFilters = addDeltaFilters(root, deltas);
        // 2. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
        // 3. Modify query - rename schemas to physical name (take into account _shard _final modifiers)
        SqlNode union = createUnionAll(withDeltaFilters, deltas);
        handler.handle(Future.succeededFuture(replaceFinalToKeyword(union)));
    }

    private SqlNode addDeltaFilters(SqlNode root, List<DeltaInformation> deltas) {
        // Recursive scan for select, on each level collect table aliases and names
        // For collected names add filter in where clause
        // TODO add support for top level order by, union all queries
        if (!(root instanceof SqlSelect)) {
            return root;
        }

        SqlNode from = ((SqlSelect) root).getFrom();
        return root;
    }

    private void setPhysicalNames(SqlNode root, List<DeltaInformation> deltas,
                                  boolean withFinalModifiers, LevelCounter counter,
                                  List<String> physicalNames) {
        if (root instanceof SqlIdentifier) {
            boolean updated = setPhysicalName(root, deltas, withFinalModifiers, counter.counter == 0, physicalNames);
            if (updated) {
                counter.counter++;
            }
        } else {
            List<SqlNode> childs = getChilds(root);
            childs.forEach(n -> setPhysicalNames(n, deltas, withFinalModifiers, counter, physicalNames));
        }
    }

    private List<SqlNode> getChilds(SqlNode root) {
        if (root instanceof SqlBasicCall) {
            return ((SqlBasicCall) root).getOperandList();
        }

        if (root instanceof SqlSelect) {
            return Collections.singletonList(((SqlSelect) root).getFrom());
        }

        if (root instanceof SqlSnapshot) {
            return Collections.singletonList(((SqlSnapshot) root).getTableRef());
        }

        if (root instanceof SqlJoin) {
            return ((SqlJoin) root).getOperandList();
        }

        return Collections.emptyList();
    }

    private boolean setPhysicalName(SqlNode root, List<DeltaInformation> deltas,
                                    boolean withFinalModifiers, boolean firstInQuery,
                                    List<String> physicalNames) {
        String schema = "";
        String id = "";
        if (((SqlIdentifier) root).names.size() == 1) {
            id = ((SqlIdentifier) root).names.get(0);
        } else {
            schema = ((SqlIdentifier) root).names.get(0);
            id = ((SqlIdentifier) root).names.get(1);
        }

        if (containsDelta(deltas, schema, id)) {
            String envPrefix = queryEnrichmentProperties.getEnvironment() + "__";
            if (schema.equals("")) {
                schema = queryEnrichmentProperties.getDefaultDatamart();
            }
            schema = envPrefix + schema;
            String postfix = firstInQuery ? "_actual" : "_actual_shard";
            if (withFinalModifiers) {
                postfix = postfix + "_final";
            }

            id = id + postfix;
            ((SqlIdentifier) root).setNames(Arrays.asList(schema, id), Arrays.asList(root.getParserPosition(), root.getParserPosition()));
            physicalNames.add(schema + "." + id);

            return true;
        }

        return false;
    }

    private boolean containsDelta(List<DeltaInformation> deltas, String schema, String name) {
        // It's better to convert List to Map, but for typical uses (less then 10 tables) full list scan is possible
        return deltas.stream().anyMatch(d -> d.getSchemaName().equals(schema) && d.getTableName().equals(name));
    }

    @SneakyThrows
    private SqlNode createSubquery(String qualifiedTableName) {
        // select 1 from tbl1_actual where sign < 0 limit 1
        return calciteContextProvider.context(null).getPlanner()
                .parse(String.format(SUBQUERY_TEMPLATE, qualifiedTableName));
    }

    private SqlNode createDeltaFilter(String tableAlias, long deltaNum, SqlParserPos pos) {
        // t.sys_from <= 98 AND t.sys_to >= 98
        // 98 between t.sys_from and t.sys_to
        SqlNode deltaLit = SqlLiteral.createExactNumeric(Long.toString(deltaNum), pos);

        return new SqlBasicCall(
                SqlStdOperatorTable.BETWEEN,
                new SqlNode[] {
                        deltaLit,
                        createId(tableAlias, "sys_from", pos),
                        createId(tableAlias, "sys_to", pos)
                },
                pos
        );
    }

    private SqlIdentifier createId(String schema, String name, SqlParserPos pos) {
        return schema.equals("") ? new SqlIdentifier(Collections.singletonList(name), pos) : new SqlIdentifier(Arrays.asList(schema, name), pos);
    }

    @SneakyThrows
    private SqlNode createUnionAll(SqlNode query, List<DeltaInformation> deltas) {
        // select * from (<query> FINAL) where <subquery filters> is not null
        // union all
        // select * from (<query>) where <subquery filters> is null
        SqlNode unionTemplate = calciteContextProvider.context(null).getPlanner().parse(UNION_ALL_TEMPLATE);
        SqlBasicCall union = (SqlBasicCall) unionTemplate;
        // This is hack because SqlNode.clone performs only top-level copying
        String sourceQuery = query.toString();

        for (int i = 0; i < union.operands.length; i++) {
            SqlSelect unionPart = (SqlSelect) union.operands[i];
            SqlNode part = parseInternalRepresentation(sourceQuery);
            boolean firstUnionPart = i == 0;
            List<String> physicalNames = new ArrayList<>();
            setPhysicalNames(part, deltas, firstUnionPart, new LevelCounter(), physicalNames);

            unionPart.setFrom(part);
            unionPart.setWhere(addSubqueryFilters(physicalNames, firstUnionPart));
        }
        return union;
    }

    @SneakyThrows
    private SqlNode addSubqueryFilters(List<String> tablesToCheck, boolean isNotNullCheck) {
        // (select 1 from tbl1_actual where sign < 0 limit 1) IS NOT NULL / IS NULL
        String template = isNotNullCheck ? SUBQUERY_NOT_NULL_CHECK : SUBQUERY_NULL_CHECK;
        SqlNode where = null;

        for (String table: tablesToCheck) {
            SqlNode subq = createSubquery(table.replaceAll("_final", ""));
            // This is sub-optimal, but required, because SqlNode.clone performs only top-level copying
            SqlBasicCall nullCheck = (SqlBasicCall) ((SqlSelect) calciteContextProvider.context(null)
                    .getPlanner().parse(template)).getWhere();

            nullCheck.setOperand(0, subq);

            SqlBinaryOperator op = isNotNullCheck ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
            where = createOperator(op, where, nullCheck);
        }

        return where;
    }

    private SqlNode createOperator(SqlBinaryOperator op, SqlNode left, SqlNode right) {
        if (left == null) {
            return right;
        }
        return new SqlBasicCall(
                op,
                new SqlNode[]{left, right},
                left.getParserPosition()
        );
    }

    // This is dirty hack, because current calcite parser didn't support FINAL keyword for Clickhouse
    // Replace `from tbl_actual_final t` => `from tbl_actual t FINAL`
    // FIXME add support for FINAL keyword into the parser
    String replaceFinalToKeyword(SqlNode root) {
        String repr = root.toString();
        StringBuilder result = new StringBuilder(repr);
        while (true) {
            Matcher m = FINAL_PATTERN.matcher(result.toString());
            if (!m.find()) {
                break;
            }

            int start = m.start();
            int end = m.end();
            String tableWithFinal = String.format("`%s`", m.group(1));
            // find next token - AS alias or anything else
            String tableAlias = tableAlias(repr, end);
            if (!tableAlias.equals("")) {
                result.delete(start, end + tableAlias.length());
                tableWithFinal = String.format("%s %s FINAL ", tableWithFinal, tableAlias);
            } else {
                // Cut final from table name
                result.delete(start, end);
                tableWithFinal = String.format("%s FINAL ", tableWithFinal);
            }
            result.insert(start, tableWithFinal);
        }
        return result.toString();
    }

    private String tableAlias(String str, int from) {
        String test = str.substring(from);
        Matcher m = TABLE_ALIAS_PATTERN.matcher(test);
        return m.find() ? m.group(1) : "";
    }

    // mutable counter for recursion calls
    private static class LevelCounter {
        public int counter = 0;
    }

    // Default Parser implementation uses \" for quoting, but default internal formatting is backtick `
    // So when we translate SqlNode.toString, and try to parse it, we should use another parser config
    private SqlParser.Config internalRepresentationConfig() {
        return SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setCaseSensitive(false)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.BACK_TICK)
                .build();
    }

    @SneakyThrows
    private SqlNode parseInternalRepresentation(String sql) {
        SqlParser parser = SqlParser.create(sql, internalRepresentationConfig());
        return parser.parseQuery();
    }
}
