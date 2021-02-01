package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableColumn;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmMetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.MetaTableEntityFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("adqmCheckTableService")
public class AdqmCheckTableService implements CheckTableService {

    public static final String SORTED_KEY_ERROR_TEMPLATE = "\tSorted keys are not equal expected [%s], got [%s].";
    private final AdqmTableEntitiesFactory adqmTableEntitiesFactory;
    private final MetaTableEntityFactory<AdqmTableEntity> metaTableEntityFactory;

    @Autowired
    public AdqmCheckTableService(AdqmTableEntitiesFactory adqmTableEntitiesFactory,
                                 MetaTableEntityFactory<AdqmTableEntity> metaTableEntityFactory) {
        this.adqmTableEntitiesFactory = adqmTableEntitiesFactory;
        this.metaTableEntityFactory = metaTableEntityFactory;
    }

    @Override
    public Future<Void> check(CheckTableRequest request) {
        AdqmTables<AdqmTableEntity> tableEntities = adqmTableEntitiesFactory
                .create(request.getEntity(), request.getEnvName());
        return Future.future(promise -> CompositeFuture.join(Stream.of(
                tableEntities.getShard(), tableEntities.getDistributed())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    String errors = list.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    if (errors.isEmpty()) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException("\n" + errors));
                    }
                })
                .onFailure(promise::fail));
    }

    private Future<Optional<String>> compare(AdqmTableEntity expTableEntity) {
        return metaTableEntityFactory.create(expTableEntity.getEnv(), expTableEntity.getSchema(),
                expTableEntity.getName())
                .compose(optTableEntity -> Future.succeededFuture(optTableEntity
                        .map(tableEntity -> compare(tableEntity, expTableEntity))
                        .orElse(Optional.of(String.format(TABLE_NOT_EXIST_ERROR_TEMPLATE, expTableEntity.getName())))));
    }

    private Optional<String> compare(AdqmTableEntity tableEntity,
                                     AdqmTableEntity expTableEntity) {

        List<String> errors = new ArrayList<>();

        List<String> expSortedKeys = Optional.ofNullable(expTableEntity.getSortedKeys())
                .orElse(Collections.emptyList());
        List<String> sortedKeys = Optional.ofNullable(tableEntity.getSortedKeys()).orElse(Collections.emptyList());
        if (!Objects.equals(expSortedKeys, sortedKeys)) {
            errors.add(String.format(SORTED_KEY_ERROR_TEMPLATE,
                    String.join(", ", expSortedKeys),
                    String.join(", ", sortedKeys)));
        }
        Map<String, AdqmTableColumn> realColumns = tableEntity.getColumns().stream()
                .collect(Collectors.toMap(AdqmTableColumn::getName, Function.identity()));
        expTableEntity.getColumns().forEach(column -> {
            AdqmTableColumn realColumn = realColumns.get(column.getName());
            if (realColumn == null) {
                errors.add(String.format(COLUMN_NOT_EXIST_ERROR_TEMPLATE, column.getName()));
            } else {
                String realType = realColumn.getType();
                String type = column.getType();
                if (!Objects.equals(type, realType)) {
                    errors.add(String.format("\tColumn `%s`:", column.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, AdqmMetaTableEntityFactory.DATA_TYPE,
                            column.getType(), realColumn.getType()));
                }
            }
        });
        return errors.isEmpty()
                ? Optional.empty()
                : Optional.of(String.format("Table `%s.%s`:\n%s",
                expTableEntity.getSchema(),
                expTableEntity.getName(),
                String.join("\n", errors)));
    }
}
