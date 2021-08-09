package io.arenadata.dtm.query.execution.plugin.adp.check.factory;

public class AdpVersionQueriesFactory {

    public static final String COMPONENT_NAME_COLUMN = "name";
    public static final String VERSION_COLUMN = "version";
    private static final String ADP_NAME = "'adp instance'";

    public static String createAdpVersionQuery() {
        return String.format("SELECT %s as %s, VERSION() as %s", ADP_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN);
    }
}
