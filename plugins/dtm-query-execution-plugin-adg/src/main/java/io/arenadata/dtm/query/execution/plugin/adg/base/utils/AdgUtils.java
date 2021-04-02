package io.arenadata.dtm.query.execution.plugin.adg.base.utils;

public class AdgUtils {
    public static final String TABLE_NAME_DELIMITER = "__";

    public static String getSpaceName(String env, String schema, String table, String postfix) {
        return String.format("%s__%s__%s%s", env, schema, table, postfix);
    }
}
