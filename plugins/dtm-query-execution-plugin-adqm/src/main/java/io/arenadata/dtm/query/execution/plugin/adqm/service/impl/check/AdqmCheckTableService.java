package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableColumn;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.CheckTableService;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("adqmCheckTableService")
public class AdqmCheckTableService implements CheckTableService {

    public static final String IS_IN_SORTING_KEY = "is_in_sorting_key";
    public static final String SORTED_KEY_ERROR_TEMPLATE = "\tSorted keys are not equal expected [%s], got [%s].";

    private static final String CONDITION_PATTERN = "WHERE table = '%s' AND database = '%s__%s'";
    public static final String QUERY_PATTERN = String.format("SELECT \n" +
                    "  name as %s, \n" +
                    "  type as %s, \n" +
                    "  is_in_sorting_key as %s\n" +
                    "FROM system.columns \n" +
                    "%s",
            COLUMN_NAME, DATA_TYPE, IS_IN_SORTING_KEY, CONDITION_PATTERN);
    private static final String REGEX_TYPE_PATTERN = "Nullable\\((.*?)\\)";
    private final DatabaseExecutor adqmQueryExecutor;
    private final AdqmTableEntitiesFactory adqmTableEntitiesFactory;

    @Autowired
    public AdqmCheckTableService(DatabaseExecutor adqmQueryExecutor,
                                 AdqmTableEntitiesFactory adqmTableEntitiesFactory) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.adqmTableEntitiesFactory = adqmTableEntitiesFactory;
    }

    @Override
    public void check(CheckContext context,
                      Handler<AsyncResult<Void>> handler) {
        AdqmTables<AdqmTableEntity> adbCreateTableQueries = adqmTableEntitiesFactory
                .create(new DdlRequestContext(new DdlRequest(context.getRequest().getQueryRequest(),
                        context.getEntity())));
        CompositeFuture.join(Stream.of(adbCreateTableQueries.getShard(), adbCreateTableQueries.getDistributed())
                .map(this::compare)
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    String errors = list.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    handler.handle(errors.isEmpty()
                            ? Future.succeededFuture()
                            : Future.failedFuture("\n" + errors));
                })
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    private Future<Optional<String>> compare(AdqmTableEntity expTableEntity) {
        return getMetadata(expTableEntity)
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
                    errors.add(String.format("\tColumn `%s` : \n", column.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, DATA_TYPE, column.getType(), realColumn.getType()));
                }
            }
        });
        return errors.isEmpty()
                ? Optional.empty()
                : Optional.of(String.format("Table `%s.%s` : \n%s",
                expTableEntity.getSchema(),
                expTableEntity.getName(),
                String.join("\n", errors)));
    }


    private Future<Optional<AdqmTableEntity>> getMetadata(AdqmTableEntity expTableEntity) {
        String query = String.format(QUERY_PATTERN, expTableEntity.getName(), expTableEntity.getEnv(),
                expTableEntity.getSchema());
        return adqmQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture(result.isEmpty()
                        ? Optional.empty()
                        : Optional.of(transformToAdqmEntity(result))));
    }

    private AdqmTableEntity transformToAdqmEntity(List<Map<String, Object>> mapList) {
        AdqmTableEntity result = new AdqmTableEntity();
        List<String> sortedKeys = new ArrayList<>();
        List<AdqmTableColumn> columns = mapList.stream()
                .peek(map -> {
                    if ("1".equals(map.get(IS_IN_SORTING_KEY).toString())) {
                        sortedKeys.add(map.get(COLUMN_NAME).toString());
                    }
                })
                .map(this::transformColumn).collect(Collectors.toList());
        result.setSortedKeys(sortedKeys);
        result.setColumns(columns);
        return result;
    }

    private AdqmTableColumn transformColumn(Map<String, Object> map) {
        String type;
        boolean nullable;
        String mapType = map.get(DATA_TYPE).toString();
        Pattern pattern = Pattern.compile(REGEX_TYPE_PATTERN);
        Matcher matcher = pattern.matcher(mapType);
        if (matcher.matches()) {
            type = matcher.group(1);
            nullable = true;
        } else {
            type = mapType;
            nullable = false;
        }
        return AdqmTableColumn.builder()
                .name(map.get(COLUMN_NAME).toString())
                .type(type)
                .nullable(nullable)
                .build();
    }
}
