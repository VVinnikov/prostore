package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.QueryEnrichmentProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class QueryRewriter {

    private final CalciteContextProvider calciteContextProvider;
    private final QueryEnrichmentProperties queryEnrichmentProperties;

    private final static String SUBQUERY_TEMPLATE = "select 1 from tbl_actual where sign < 0 limit 1";
    private final static Pattern FINAL_PATTERN = Pattern.compile("`(\\w+)_final`", Pattern.CASE_INSENSITIVE);
    private final static Pattern TABLE_ALIAS_PATTERN = Pattern.compile("^\\s+(AS\\s+`\\w+`)");

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
        // 2. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
        // 3. Modify query - rename schemas to physical name

        handler.handle(Future.succeededFuture(replaceFinalToKeyword(root)));
    }

    private SqlNode createSubquery(String qualifiedTableName, SqlParserPos parserPos) {
        // select 1 from tbl1_actual where sign < 0 limit 1
        return null; //SqlSelect result = new SqlSelect();
    }

    private SqlNode createSubqueryFilter(SqlNode subquery, boolean isNotNullCheck) {
        // (select 1 from tbl1_actual where sign < 0 limit 1) IS NOT NULL / IS NULL
        return null;
    }

    private SqlNode createDeltaFilter(String tableAlias, long deltaNum) {
        // t.sys_from <= 98 AND t.sys_to >= 98
        // 98 between t.sys_from and t.sys_to
        return null;
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
}
