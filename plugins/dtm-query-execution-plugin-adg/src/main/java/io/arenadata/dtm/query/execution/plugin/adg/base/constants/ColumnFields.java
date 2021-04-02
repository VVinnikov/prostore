package io.arenadata.dtm.query.execution.plugin.adg.base.constants;

public class ColumnFields {

  /** Идентификатор */
  public final static String ID = "id";
  /** Идентификатор для навигации по запросов с роутера */
  public final static String BUCKET_ID = "bucket_id";
  /** Название текущей таблицы */
  public static final String ACTUAL_POSTFIX = "_actual";
  /** Название staging таблицы */
  public static final String STAGING_POSTFIX = "_staging";
  /** Название таблицы истории */
  public static final String HISTORY_POSTFIX = "_history";
  /** Системное поле операции над объектом */
  public static final String SYS_OP_FIELD = "sys_op";
  /** Системное поле номера дельты */
  public static final String SYS_FROM_FIELD = "sys_from";
  /** Системное поле максимального номера дельты */
  public static final String SYS_TO_FIELD = "sys_to";
}
