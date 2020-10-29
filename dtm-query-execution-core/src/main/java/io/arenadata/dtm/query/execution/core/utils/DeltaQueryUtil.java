package io.arenadata.dtm.query.execution.core.utils;

import java.time.format.DateTimeFormatter;

public class DeltaQueryUtil {

    public static final String DELTA_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss[.SSS]";
    public static final DateTimeFormatter DELTA_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DELTA_DATE_TIME_PATTERN);
    public static final String NUM_FIELD = "delta_num";
    public static final String CN_FROM_FIELD = "cn_to";
    public static final String CN_TO_FIELD = "cn_from";
    public static final String DATE_TIME_FIELD = "delta_date";
    public static final String CN_MAX_FIELD = "cn_max";
    public static final String IS_ROLLING_BACK_FIELD = "is_rolling_back";
    public static final String WRITE_OP_FINISHED_FIELD = "write_op_finished";
}
