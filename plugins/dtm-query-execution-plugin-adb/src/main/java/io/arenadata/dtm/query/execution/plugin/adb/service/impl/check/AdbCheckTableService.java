package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TableEntitiesFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("adbCheckTableService")
public class AdbCheckTableService implements CheckTableService {

    public static final String CHARACTER_MAXIMUM_LENGTH = "character_maximum_length";
    public static final String DATETIME_PRECISION = "datetime_precision";
    public static final String ORDINAL_POSITION = "ordinal_position";
    public static final String COLUMN_NAME = "column_name";
    public static final String CONSTRAINT_TYPE = "constraint_type";
    public static final String DATA_TYPE = "data_type";
    public static final String PRIMARY_KEY_CS_TYPE = "PRIMARY KEY";
    public static final String FIELD_ERROR_TEMPLATE = "\t\t`%s` expected `%s` got `%s`.";
    public static final String TABLE_NOT_EXIST_ERROR_TEMPLATE = "Table ‘%s’ does not exist.";
    public static final String COLUMN_NOT_EXIST_ERROR_TEMPLATE = "\tColumn '%s' does not exist.";
    public static final String PRIMARY_KEY_ERROR_TEMPLATE = "\tPrimary keys are not equal expected [%s], got [%s].";

    private static final String QUERY_PATTERN = String.format("SELECT \n" +
                    "  c.column_name as %s, \n" +
                    "  c.udt_name as %s, \n" +
                    "  con.constraint_type as %s,\n" +
                    "  c.%s,\n" +
                    "  c.%s,\n" +
                    "  u.%s\n" +
                    " FROM INFORMATION_SCHEMA.COLUMNS c\n" +
                    "    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE u\n" +
                    "      ON u.table_schema = c.table_schema\n" +
                    "        AND u.table_name = c.table_name\n" +
                    "        AND u.column_name = c.column_name\n" +
                    "    LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS con\n" +
                    "      ON con.constraint_schema = u.constraint_schema\n" +
                    "        AND con.table_schema = u.table_schema\n" +
                    "        AND con.constraint_catalog = u.constraint_catalog\n" +
                    "        AND con.table_name = u.table_name\n",
            COLUMN_NAME, DATA_TYPE, CONSTRAINT_TYPE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, ORDINAL_POSITION);

    public static final String QUERY_PATTERN_WITH_CONDITION = QUERY_PATTERN + " WHERE c.table_schema = '%s'\n" +
            "  AND c.table_name = '%s';";

    private final DatabaseExecutor adbQueryExecutor;
    private final TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory;

    @Autowired
    public AdbCheckTableService(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor,
                                TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public void check(CheckContext context,
                      Handler<AsyncResult<Void>> handler) {
        AdbTables<AdbTableEntity> adbCreateTableQueries = tableEntitiesFactory
                .create(new DdlRequestContext(new DdlRequest(context.getRequest().getQueryRequest(),
                        context.getEntity())));
        CompositeFuture.join(Stream.of(adbCreateTableQueries.getActual(), adbCreateTableQueries.getHistory(),
                adbCreateTableQueries.getStaging())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<String> list = result.list();
                    if (list.stream().allMatch(String::isEmpty)) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        handler.handle(Future.failedFuture("\n" + String.join("\n", list)));
                    }
                })
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    private Future<String> compare(AdbTableEntity expTableEntity) {
        return getMetadata(expTableEntity)
                .compose(optTableEntity -> Future.succeededFuture(optTableEntity
                        .map(tableEntity -> compare(tableEntity, expTableEntity))
                        .orElse(String.format(TABLE_NOT_EXIST_ERROR_TEMPLATE, expTableEntity.getName()))));
    }

    private String compare(AdbTableEntity tableEntity,
                           AdbTableEntity expTableEntity) {

        List<String> errors = new ArrayList<>();
        if (!Objects.equals(expTableEntity.getPrimaryKeys(), tableEntity.getPrimaryKeys())) {
            errors.add(String.format(PRIMARY_KEY_ERROR_TEMPLATE,
                    String.join(", ", expTableEntity.getPrimaryKeys()),
                    String.join(", ", tableEntity.getPrimaryKeys())));
        }
        Map<String, AdbTableColumn> realColumns = tableEntity.getColumns().stream()
                .collect(Collectors.toMap(AdbTableColumn::getName, Function.identity()));
        expTableEntity.getColumns().forEach(column -> {
            AdbTableColumn realColumn = realColumns.get(column.getName());
            if (realColumn == null) {
                errors.add(String.format(COLUMN_NOT_EXIST_ERROR_TEMPLATE, column.getName()));
            } else {
                String realType = realColumn.getType();
                String type = column.getType();
                if (!Objects.equals(type, realType)) {
                    errors.add(String.format("\tColumn `%s` : \n", column.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, DATA_TYPE, column.getType(), realColumn.getType()));
                }
            }
        });
        return errors.isEmpty()
                ? ""
                : String.format("Table `%s.%s` : \n%s", expTableEntity.getSchema(), expTableEntity.getName(),
                String.join("\n", errors));
    }

    private Future<Optional<AdbTableEntity>> getMetadata(AdbTableEntity expTableEntity) {
        String query = String.format(QUERY_PATTERN_WITH_CONDITION, expTableEntity.getSchema(), expTableEntity.getName());
        return adbQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture(result.isEmpty()
                        ? Optional.empty()
                        : Optional.of(transformToAdbEntity(result))));
    }

    private AdbTableEntity transformToAdbEntity(List<Map<String, Object>> mapList) {
        AdbTableEntity result = new AdbTableEntity();
        Map<Integer, String> pkKeys = new TreeMap<>();
        List<AdbTableColumn> columns = mapList.stream()
                .filter(map -> Optional.ofNullable(map.get(CONSTRAINT_TYPE))
                        .map(PRIMARY_KEY_CS_TYPE::equals)
                        .orElse(true))
                .peek(map -> Optional.ofNullable(map.get(ORDINAL_POSITION))
                        .ifPresent(pos -> pkKeys.put(Integer.parseInt(pos.toString()), map.get(COLUMN_NAME).toString())))
                .map(map -> new AdbTableColumn(map.get(COLUMN_NAME).toString(), getType(map), false))
                .collect(Collectors.toList());
        result.setColumns(columns);
        result.setPrimaryKeys(new ArrayList<>(pkKeys.values()));
        return result;
    }

    private String getType(Map<String, Object> map) {

        String type = map.get(DATA_TYPE).toString();
        String size = Optional.ofNullable(map.get(CHARACTER_MAXIMUM_LENGTH))
                .map(val -> String.format("(%s)", val))
                .orElse("");
        String precision = Optional.ofNullable(map.get(DATETIME_PRECISION))
                .map(val -> String.format("(%s)", val))
                .orElse("");
        switch (type) {
            case "varchar":
            case "char":
                type = String.format("%s%s", type, size);
                break;
            case "time":
            case "timestamp":
                type = String.format("%s%s", type, precision);
                break;
        }
        return type;
    }
}
