package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;

@Component
@Slf4j
public class QueryRewriter {

    private final static String SUBQUERY_TEMPLATE = "select 1 from %s where sign < 0 limit 1";
    private final static String SUBQUERY_NOT_NULL_CHECK = "select 1 from dual where (select 1 from dual) is not null";
    private final static String SUBQUERY_NULL_CHECK = "select 1 from dual where (select 1 from dual) is null";
    private final static Pattern FINAL_PATTERN = Pattern.compile("`(\\w+)_final`", Pattern.CASE_INSENSITIVE);
    private final static Pattern TABLE_ALIAS_PATTERN = Pattern.compile("^\\s+(AS\\s+`\\w+`)");
    private final static String UNION_ALL_TEMPLATE = "select * from (select 1 from dual) union all select * from (select 1 from dual)";
    private final AdqmCalciteContextProvider calciteContextProvider;
    private final AppConfiguration appConfiguration;

    public QueryRewriter(AdqmCalciteContextProvider calciteContextProvider, AppConfiguration appConfiguration) {
        this.calciteContextProvider = calciteContextProvider;
        this.appConfiguration = appConfiguration;
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
        // 2. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
        // 3. Modify query - rename schemas to physical name (take into account _shard _final modifiers)
        SqlNode union = createUnionAll(root, deltas);
        handler.handle(Future.succeededFuture(
                replaceAnsiToSpecific(
                        replaceFinalToKeyword(union.toString()))));
    }

    private SqlNode addDeltaFilters(SqlNode root, List<DeltaInformation> deltas) {
        // Recursive scan for select, on each level collect table aliases and names
        // For collected names add filter in where clause
        // Add clause in where, change SqlSnapshot to SqlIdentifier to remove for system_time clause
        List<ParseTableContext> ctxs = new ArrayList<>();
        scanForTables(root, ctxs);

        for (ParseTableContext ctx : ctxs) {
            for (TableWithAlias t : ctx.tables) {
                Long delta = findDelta(deltas, t.id, t.alias);
                if (delta != null) {
                    Pair<String, String> parsed = fromSqlIdentifier(t.id);
                    String alias = t.alias.equals("") ? parsed.right : t.alias;
                    SqlNode filter = createDeltaFilter(alias.replaceAll("_final", ""), delta, ctx.current.getParserPosition());
                    ctx.current.setWhere(createOperator(SqlStdOperatorTable.AND, ctx.current.getWhere(), filter));
                }
            }
        }

        return root;
    }

    private void scanForTables(SqlNode root, List<ParseTableContext> accum) {
        // First entry, initialize context
        ParseTableContext ctx = new ParseTableContext();
        ctx.current = (SqlSelect) root;
        accum.add(ctx);

        // Work with last created context
        ParseTableContext currentCtx = accum.get(accum.size() - 1);
        SqlNode from = currentCtx.current.getFrom();

        TableWithAlias t;
        t = tableFromNode(from, accum);
        if (t != null) {
            currentCtx.tables.add(t);
        }

        if (from instanceof SqlSelect) { // select from <query>
            scanForTables(from, accum);
        }

        if (from instanceof SqlJoin) {
            SqlNode currentJoin = from;
            while (true) {
                t = tableFromNode(((SqlJoin) currentJoin).getRight(), accum);
                if (t != null) {
                    currentCtx.tables.add(t);
                }

                t = tableFromNode(((SqlJoin) currentJoin).getLeft(), accum);
                if (t != null) {
                    currentCtx.tables.add(t);
                    break; // all join sides are tables
                }

                if (((SqlJoin) currentJoin).getLeft() instanceof SqlJoin) {
                    currentJoin = ((SqlJoin) currentJoin).getLeft();
                } else {
                    break;
                }
            }
        }
    }

    private TableWithAlias tableFromNode(SqlNode node, List<ParseTableContext> accum) {
        if (node instanceof SqlIdentifier) { // select from table
            return new TableWithAlias((SqlIdentifier) node, "");
        }

        if (node instanceof SqlSnapshot) { // select from table for system_time
            return new TableWithAlias((SqlIdentifier) ((SqlSnapshot) node).getTableRef(), "");
        }

        if (node instanceof SqlBasicCall) {
            if (node.getKind() == SqlKind.AS) { // select from table as t && select from (select from) as
                Pair<String, String> parsed = fromSqlIdentifier((SqlIdentifier) ((SqlBasicCall) node).operands[1]);
                String alias = parsed.right;
                TableWithAlias tt = tableFromNode(((SqlBasicCall) node).operands[0], accum);
                if (tt != null) {
                    tt.alias = alias;
                    return tt;
                }

                if (((SqlBasicCall) node).operands[0] instanceof SqlSelect) {
                    // we need to go to the next level
                    ParseTableContext newCtx = new ParseTableContext();
                    newCtx.current = (SqlSelect) ((SqlBasicCall) node).operands[0];
                    scanForTables(((SqlBasicCall) node).operands[0], accum);
                }
            }
        }

        return null;
    }

    private Pair<String, String> fromSqlIdentifier(SqlIdentifier node) {
        String schema = "";
        String id;
        if (node.names.size() == 1) {
            id = node.names.get(0);
        } else {
            schema = node.names.get(0);
            id = node.names.get(1);
        }

        return Pair.of(schema, id);
    }

    private boolean containsDelta(List<DeltaInformation> deltas, String schema, String name) {
        // It's better to convert List to Map, but for typical uses (less then 10 tables) full list scan is possible
        return deltas.stream().anyMatch(d -> d.getSchemaName().equals(schema) && d.getTableName().equals(name));
    }

    private void updateDelta(List<DeltaInformation> deltas,
                             Pair<String, String> datamartName,
                             Pair<String, String> physicalName) {
        // FIXME This will fail if one table use more than once in the query with different aliases
        // Like accounts_actual a1 join accounts_actual_shard a2
        deltas.forEach(d -> {
            if (d.getSchemaName().equals(datamartName.left) && d.getTableName().equals(datamartName.right)) {
                d.setSchemaName(physicalName.left);
                d.setTableName(physicalName.right);
            }
        });
    }

    private Long findDelta(List<DeltaInformation> deltas, SqlIdentifier table, String alias) {
        Pair<String, String> parsed = fromSqlIdentifier(table);
        // It's better to convert List to Map, but for typical uses (less then 10 tables) full list scan is possible
        for (DeltaInformation d : deltas) {
            if (d.getSchemaName().equals(parsed.left) &&
                    d.getTableName().equals(parsed.right.replaceAll("_final", "")) &&
                    d.getTableAlias().equals(alias)) {
                return d.getDeltaNum();
            }
        }

        return null;
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
                new SqlNode[]{
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
        val unionTemplate = calciteContextProvider.context(null).getPlanner().parse(UNION_ALL_TEMPLATE);
        val union = (SqlBasicCall) unionTemplate;
        // This is hack because SqlNode.clone performs only top-level copying
        val sourceQuery = query.toString();

        for (int i = 0; i < union.operands.length; i++) {
            // Because we mutates original deltas to physical table names
            val localDeltas = deltas.stream().map(DeltaInformation::copy).collect(Collectors.toList());
            val unionPart = (SqlSelect) union.operands[i];
            val part = parseInternalRepresentation(sourceQuery);
            val firstUnionPart = i == 0;
            val physicalNames = new ArrayList<String>();
            setPhysicalName(part,
                    firstUnionPart,
                    physicalNames,
                    localDeltas.iterator());
            val withDeltaFilters = addDeltaFilters(part, localDeltas);
            unionPart.setFrom(withDeltaFilters);
            unionPart.setWhere(addSubqueryFilters(physicalNames, firstUnionPart));
        }
        return union;
    }

    private void setPhysicalName(SqlNode part,
                                 boolean firstUnionPart,
                                 List<String> physicalNames,
                                 Iterator<DeltaInformation> deltaIterator) {
        val tables = new SqlSelectTree(part)
                .findAllTableAndSnapshots();
        for (int tablePos = 0; tablePos < tables.size(); tablePos++) {
            val table = tables.get(tablePos);
            setPhysicalName(table.getNode(),
                    deltaIterator.next(),
                    firstUnionPart,
                    tablePos == 0,
                    physicalNames);
        }
    }

    private void setPhysicalName(SqlIdentifier root, DeltaInformation delta,
                                 boolean withFinalModifiers, boolean firstInQuery,
                                 List<String> physicalNames) {
        String schema = delta.getSchemaName();
        String id = delta.getTableName();
        String envPrefix = appConfiguration.getSystemName() + "__";
        if (schema.equals("")) {
            schema = appConfiguration.getDefaultDatamart();
        }
        schema = envPrefix + schema;
        String postfix = firstInQuery ? "_actual" : "_actual_shard";
        if (withFinalModifiers) {
            postfix = postfix + "_final";
        }
        id = id + postfix;
        root.setNames(Arrays.asList(schema, id), Arrays.asList(root.getParserPosition(), root.getParserPosition()));
        physicalNames.add(schema + "." + id);
        delta.setSchemaName(schema);
        delta.setTableName(id.replaceAll("_final", ""));
    }

    @SneakyThrows
    private SqlNode addSubqueryFilters(List<String> tablesToCheck, boolean isNotNullCheck) {
        // (select 1 from tbl1_actual where sign < 0 limit 1) IS NOT NULL / IS NULL
        String template = isNotNullCheck ? SUBQUERY_NOT_NULL_CHECK : SUBQUERY_NULL_CHECK;
        SqlNode where = null;

        for (String table : tablesToCheck) {
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
    String replaceFinalToKeyword(String root) {
        StringBuilder result = new StringBuilder(root);
        while (true) {
            Matcher m = FINAL_PATTERN.matcher(result.toString());
            if (!m.find()) {
                break;
            }

            int start = m.start();
            int end = m.end();
            String tableWithFinal = String.format("`%s`", m.group(1));
            // find next token - AS alias or anything else
            String tableAlias = tableAlias(result.toString(), end);
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
        return m.find() ? m.group(0) : "";
    }

    private String replaceAnsiToSpecific(String query) {
        // Change fetch 1 next to limit 1
        return query
                .replaceAll("ASYMMETRIC", "")
                .replaceAll("FETCH NEXT 1 ROWS ONLY", "LIMIT 1");
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

    @AllArgsConstructor
    @NoArgsConstructor
    private static class ParseTableContext {
        SqlSelect current;
        List<TableWithAlias> tables = new ArrayList<>();
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class TableWithAlias {
        SqlIdentifier id;
        String alias;
    }
}
