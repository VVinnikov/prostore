package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableColumn;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTableEntity;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbMetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.factory.MetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("adbCheckTableService")
public class AdbCheckTableService implements CheckTableService {
    public static final String PRIMARY_KEY_ERROR_TEMPLATE = "\tPrimary keys are not equal expected [%s], got [%s].";

    private final TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory;
    private final MetaTableEntityFactory<AdbTableEntity> metaTableEntityFactory;

    @Autowired
    public AdbCheckTableService(TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory,
                                MetaTableEntityFactory<AdbTableEntity> metaTableEntityFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
        this.metaTableEntityFactory = metaTableEntityFactory;
    }

    @Override
    public Future<Void> check(CheckContext context) {
        AdbTables<AdbTableEntity> adbCreateTableQueries = tableEntitiesFactory
                .create(context.getEntity(), context.getRequest().getQueryRequest().getEnvName());
        return Future.future(promise -> CompositeFuture.join(Stream.of(
                adbCreateTableQueries.getActual(),
                adbCreateTableQueries.getHistory(),
                adbCreateTableQueries.getStaging())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    String errors = list.stream().filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    if (errors.isEmpty()) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException("\n" + errors));
                    }
                })
                .onFailure(promise::fail)
        );
    }

    private Future<Optional<String>> compare(AdbTableEntity expTableEntity) {
        return metaTableEntityFactory.create(null, expTableEntity.getSchema(), expTableEntity.getName())
                .compose(optTableEntity -> Future.succeededFuture(optTableEntity
                        .map(tableEntity -> compare(tableEntity, expTableEntity))
                        .orElse(Optional.of(String.format(TABLE_NOT_EXIST_ERROR_TEMPLATE, expTableEntity.getName())))));
    }

    private Optional<String> compare(AdbTableEntity tableEntity,
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
                    errors.add(String.format("\tColumn `%s`:", column.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, AdbMetaTableEntityFactory.DATA_TYPE,
                            column.getType(), realColumn.getType()));
                }
            }
        });
        return errors.isEmpty()
                ? Optional.empty()
                : Optional.of(String.format("Table `%s.%s`:\n%s", expTableEntity.getSchema(),
                expTableEntity.getName(), String.join("\n", errors)));
    }
}
