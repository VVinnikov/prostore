package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl.AdgCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.CheckTableService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("adgCheckTableService")
public class AdgCheckTableService implements CheckTableService {
    public static final String SPACE_INDEXES_ERROR_TEMPLATE = "\tSpace indexes are not equal expected [%s], got [%s].";
    private final AdgCartridgeClient adgCartridgeClient;
    private final AdgCreateTableQueriesFactory adgCreateTableQueriesFactory;

    @Autowired
    public AdgCheckTableService(AdgCartridgeClient adgCartridgeClient,
                                AdgCreateTableQueriesFactory adgCreateTableQueriesFactory) {
        this.adgCartridgeClient = adgCartridgeClient;
        this.adgCreateTableQueriesFactory = adgCreateTableQueriesFactory;
    }

    @Override
    public void check(CheckContext context, Handler<AsyncResult<Void>> handler) {
        AdgCreateTableQueries adgCreateTableQueries = adgCreateTableQueriesFactory
                .create(new DdlRequestContext(new DdlRequest(context.getRequest().getQueryRequest(),
                        context.getEntity())));
        Map<String, Space> expSpaces = Stream.of(
                adgCreateTableQueries.getActual(),
                adgCreateTableQueries.getHistory(),
                adgCreateTableQueries.getStaging())
                .collect(Collectors.toMap(AdgSpace::getName, AdgSpace::getSpace));
        adgCartridgeClient.getSpaceDescriptions(expSpaces.keySet())
                .onSuccess(spaces -> {
                    String errors = expSpaces.entrySet().stream()
                            .map(entry -> compare(entry.getKey(), spaces.get(entry.getKey()), entry.getValue()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining("\n"));
                    if (errors.isEmpty()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        handler.handle(Future.failedFuture("\n" + errors));
                    }
                })
                .onFailure(error -> handler.handle(Future.failedFuture(error.getMessage())));
    }

    private Optional<String> compare(String spaceName, Space space, Space expSpace) {
        List<String> errors = new ArrayList<>();

        if (!Objects.equals(getIndexNames(space), getIndexNames(expSpace))) {
            errors.add(String.format(SPACE_INDEXES_ERROR_TEMPLATE,
                    space.getIndexes().stream()
                            .map(SpaceIndex::getName)
                            .collect(Collectors.joining(", ")),
                    expSpace.getIndexes().stream()
                            .map(SpaceIndex::getName)
                            .collect(Collectors.joining(", "))));
        }

        expSpace.getFormat().forEach(expAttr -> {
            Optional<SpaceAttribute> optAttr = space.getFormat().stream()
                    .filter(attr -> attr.getName().equals(expAttr.getName()))
                    .findAny();
            if (optAttr.isPresent()) {
                SpaceAttributeTypes type = optAttr.get().getType();
                if (!Objects.equals(type, expAttr.getType())) {
                    errors.add(String.format("\tColumn `%s` : \n", expAttr.getName()));
                    errors.add(String.format(FIELD_ERROR_TEMPLATE, DATA_TYPE, expAttr.getType().getName(),
                            type.getName()));
                }
            } else {
                errors.add(String.format(COLUMN_NOT_EXIST_ERROR_TEMPLATE, expAttr.getName()));
            }
        });

        return errors.isEmpty()
                ? Optional.empty()
                : Optional.of(String.format("\n Table `%s`:\n%s", spaceName, String.join("\n", errors)));
    }

    private List<String> getIndexNames(Space space) {
        return space.getIndexes().stream()
                .map(SpaceIndex::getName)
                .collect(Collectors.toList());
    }
}
