package ru.ibs.dtm.query.execution.plugin.adqm.service.impl;

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

    public static final Set<String> SYSTEM_FIELDS = new HashSet<>(Arrays.asList(
            "sys_from", "sys_to", "sys_op", "close_date", "sign"
    ));

}
