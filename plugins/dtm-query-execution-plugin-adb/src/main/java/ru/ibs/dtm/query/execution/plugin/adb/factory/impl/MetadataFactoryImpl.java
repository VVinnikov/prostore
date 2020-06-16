package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypeUtil;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;

import java.util.Collection;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class MetadataFactoryImpl implements MetadataFactory {

  /**
   * Название таблицы актуальных данных
   */
  public final static String ACTUAL_TABLE = "actual";
  /**
   * Название таблицы истории
   */
  public final static String HISTORY_TABLE = "history";
  /**
   * Название стейджинг таблицы
   */
  public final static String STAGING_TABLE = "staging";
  /**
   * Системное поле номера дельты
   */
  public final static String SYS_FROM_ATTR = "sys_from";
  /**
   * Системное поле максимального номера дельты
   */
  public final static String SYS_TO_ATTR = "sys_to";
  /**
   * Системное поле операции над объектом
   */
  public final static String SYS_OP_ATTR = "sys_op";
  /**
   * Системное поле идентификатора запроса
   */
  public final static String REQ_ID_ATTR = "req_id";

  private final static String DROP_TABLE = "DROP TABLE IF EXISTS ";

  private AdbQueryExecutor adbQueryExecutor;

  public MetadataFactoryImpl(AdbQueryExecutor adbQueryExecutor) {
    this.adbQueryExecutor = adbQueryExecutor;
  }

  @Override
  public void apply(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    String dropSql = dropTableScript(classTable);
    adbQueryExecutor.executeUpdate(dropSql, ar -> {
      if (ar.succeeded()) {
        String createSql = createTableScripts(classTable);
        adbQueryExecutor.executeUpdate(createSql, handler);
      } else {
        log.error("Ошибка исполнения метода apply плагина ADB", ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public void purge(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    String dropSql = dropTableScript(classTable);
    adbQueryExecutor.executeUpdate(dropSql, handler);
  }

  private String createTableScripts(ClassTable classTable) {
    StringBuilder sb = new StringBuilder()
      .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + ACTUAL_TABLE, false))
      .append("; ")
      .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + HISTORY_TABLE, false))
      .append("; ")
      .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + STAGING_TABLE, true))
      .append("; ");
    return sb.toString();
  }

  private String dropTableScript(ClassTable classTable) {
    StringBuilder sb = new StringBuilder()
      .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(ACTUAL_TABLE)
      .append("; ")
      .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(HISTORY_TABLE)
      .append("; ")
      .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(STAGING_TABLE)
      .append("; ");
    return sb.toString();
  }

  private String createTableScript(ClassTable classTable, String tableName, boolean addRegId) {
    StringBuilder sb = new StringBuilder()
      .append("CREATE TABLE ").append(tableName)
      .append(" (");
    final String[] prefix = {" "};
    classTable.getFields().forEach(it -> {
      sb.append(prefix[0])
        .append(it.getName())
        .append(" ")
        .append(ClassTypeUtil.pgFromMariaType(it.getType().name().toLowerCase()));
      if (it.getType().name().equalsIgnoreCase("varchar") && it.getSize() != null) {
        sb.append("(").append(it.getSize()).append(") ");
      } else {
        sb.append(" ");
      }
      if (!it.getNull()) {
        sb.append("NOT NULL");
      }
      prefix[0] = ", ";
    });
    sb.append(prefix[0])
      .append(SYS_FROM_ATTR)
      .append(" ")
      .append("bigint")
      .append(prefix[0])
      .append(SYS_TO_ATTR)
      .append(" ")
      .append("bigint")
      .append(prefix[0])
      .append(SYS_OP_ATTR)
      .append(" ")
      .append("int");
    if (addRegId) {
      sb.append(prefix[0])
        .append(REQ_ID_ATTR)
        .append(" ")
        .append("varchar(36)");
    }
    Collection<ClassField> pkList = classTable.getFields().stream().filter(f -> f.getPrimaryOrder() != null).collect(toList());
    if (pkList.size() > 0) {
      sb.append(prefix[0])
              .append("constraint ")
              .append("pk_")
              .append(tableName.replace('.', '_'))
              .append(" primary key (")
              .append(pkList.stream().map(ClassField::getName).collect(Collectors.joining(", ")))
              .append(")");
    }
    sb.append(")");
    return sb.toString();
  }
}
