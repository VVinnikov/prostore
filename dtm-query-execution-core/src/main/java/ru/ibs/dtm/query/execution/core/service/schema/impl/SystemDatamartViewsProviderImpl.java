package ru.ibs.dtm.query.execution.core.service.schema.impl;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.servicedb.ViewDao;
import ru.ibs.dtm.query.execution.core.dto.SystemDatamartView;
import ru.ibs.dtm.query.execution.core.service.schema.SystemDatamartViewsProvider;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class SystemDatamartViewsProviderImpl implements SystemDatamartViewsProvider {
    public static final String MNEMONIC = "information_schema";
    private final ViewDao viewDao;
    private List<SystemDatamartView> systemViews;
    private List<Datamart> datamartList;

    public SystemDatamartViewsProviderImpl(ViewDao viewDao) {
        this.viewDao = viewDao;
    }

    @Override
    public List<SystemDatamartView> getSystemViews() {
        return systemViews;
    }

    @Override
    public List<Datamart> getLogicalSchemaFromSystemViews() {
        if (datamartList != null) {
            return datamartList;
        } else {
            throw new RuntimeException("System Views is not loaded");
        }
    }

    @Override
    public Future<Void> fetchSystemViews() {
        return Future.future(p -> viewDao.findAllSystemViews(ar -> {
            if (ar.succeeded()) {
                try {
                    systemViews = ar.result();
                    datamartList = getDatamartList(systemViews);
                } catch (Exception ex) {
                    log.error("Schema create error: ", ex);
                    p.handle(Future.failedFuture(ex));
                }
            } else {
                log.error("System views fetch error: ", ar.cause());
                p.handle(Future.failedFuture(ar.cause()));
            }
        }));
    }

    @NotNull
    private List<Datamart> getDatamartList(List<SystemDatamartView> systemViews) {
        val systemDatamart = new Datamart();
        systemDatamart.setDatamartTables(systemViews.stream()
            .map(SystemDatamartView::getDatamartTable)
            .collect(Collectors.toList()));
        systemDatamart.setId(UUID.randomUUID());
        systemDatamart.setMnemonic(MNEMONIC);
        return Collections.singletonList(systemDatamart);
    }

}
