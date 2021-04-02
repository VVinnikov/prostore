package io.arenadata.dtm.query.execution.plugin.adqm.base.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class Constants {
    private Constants() {}

    public static final String ACTUAL_POSTFIX = "_actual";
    public static final String ACTUAL_SHARD_POSTFIX = "_actual_shard";
    public static final String ACTUAL_LOADER_SHARD_POSTFIX = "_actual_loader_shard";
    public static final String BUFFER_POSTFIX = "_buffer";
    public static final String BUFFER_SHARD_POSTFIX = "_buffer_shard";
    public static final String BUFFER_LOADER_SHARD_POSTFIX = "_buffer_loader_shard";
    public static final String EXT_SHARD_POSTFIX = "_ext_shard";
    public static final String SYS_FROM_FIELD = "sys_from";
    public static final String SYS_TO_FIELD = "sys_to";
    public static final String SYS_OP_FIELD = "sys_op";
    public static final String SYS_CLOSE_DATE_FIELD = "sys_close_date";
    public static final String SIGN_FIELD = "sign";
    public static final Set<String> SYSTEM_FIELDS = new HashSet<>(Arrays.asList(
            SYS_FROM_FIELD, SYS_TO_FIELD, SYS_OP_FIELD, SYS_CLOSE_DATE_FIELD, SIGN_FIELD
    ));

    public static String getDbName(String envName, String datamartMnemonic) {
        return String.format("%s__%s", envName, datamartMnemonic);
    }

}
