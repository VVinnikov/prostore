package ru.ibs.dtm.query.execution.core.utils;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Quoting;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SqlPreparer {

    public static final String UNABLE_TO_GET_VIEW_NAME = "Unable to get view name";
    public static final String VIEW_NAME_PATH = "_VIEW.IDENTIFIER";
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile("(?<=\\stable\\s)([A-z.0-9\"]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_DISTRIBUTED_TABLE_PATTERN = Pattern.compile("(DISTRIBUTED BY.+$)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_TABLE_EXISTS_PATTERN = Pattern.compile("(?<=\\stable if not exists\\s)([A-z.0-9\"]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHECK_CREATE_OR_REPLACE_PATTERN = Pattern.compile("(?i)^(\\s+)?CREATE\\s+OR\\s+REPLACE");
    private static final Pattern CHECK_ALTER_PATTERN = Pattern.compile("(?i)^(\\s+)?ALTER");
    private static final Pattern GET_VIEW_QUERY_PATTERN = Pattern.compile("(?i)view\\s+\\w+\\s+as\\s+(SELECT.*)");
    private static final String SERVICE_DB_NAME = "dtmservice";

    /**
     * Определяем схему и таблицу, где будем создавать физическую "пустышку".
     *
     * @param targetSchema схема
     * @param table        название таблицы
     * @return таблица с правильной схемой
     */
    public static String getTableWithSchema(String targetSchema, String table) {
        int indexComma = table.indexOf(".");
        if (indexComma != -1) {
            String schema = StringUtils.isEmpty(targetSchema) ? table.substring(0, indexComma) : targetSchema;
            String name = table.substring(indexComma + 1);
            return schema + "." + name;
        } else {
            String schema = StringUtils.isEmpty(targetSchema) ? SERVICE_DB_NAME : targetSchema;
            return schema + "." + table;
        }
    }

    /**
     * Заменяет название таблицы в запросе, если таблица пришла без схемы
     *
     * @param sql             запрос
     * @param tableWithSchema таблица со схемой
     * @return обогащенный запрос
     */
    public static String replaceTableInSql(String sql, String tableWithSchema) {
        if (sql.toLowerCase().contains(tableWithSchema.toLowerCase())) {
            return sql;
        }
        Matcher matcher = CREATE_TABLE_EXISTS_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceAll(tableWithSchema);
        }
        matcher = CREATE_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceAll(tableWithSchema);
        }
        return sql;
    }

    /**
     * Заменяем двойные кавычки на обратные, т.к. такие используются в марии
     *
     * @param sql запрос
     * @return запрос с корректными кавычками для марии
     */
    public static String replaceQuote(String sql) {
        return sql.replace(Quoting.DOUBLE_QUOTE.string, Quoting.BACK_TICK.string);
    }

    public static String removeDistributeBy(String sql) {
        Matcher matcher = CREATE_DISTRIBUTED_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.replaceFirst("");
        }
        return sql;
    }

    public static String getViewName(SqlSelectTree tree) {
        val namesByView = tree.findNodesByPath(VIEW_NAME_PATH);
        if (namesByView.isEmpty()) {
            throw new IllegalArgumentException(UNABLE_TO_GET_VIEW_NAME);
        } else {
            return namesByView.get(0).tryGetTableName().orElseThrow(() -> new IllegalArgumentException(UNABLE_TO_GET_VIEW_NAME));
        }
    }

    public static boolean isCreateOrReplace(String sql) {
        return CHECK_CREATE_OR_REPLACE_PATTERN.matcher(sql).find();
    }

    public static boolean isAlter(String sql) {
        return CHECK_ALTER_PATTERN.matcher(sql).find();
    }
}
