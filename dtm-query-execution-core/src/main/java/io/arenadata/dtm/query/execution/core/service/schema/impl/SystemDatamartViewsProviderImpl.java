package io.arenadata.dtm.query.execution.core.service.schema.impl;

import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.SystemDatamartView;
import io.arenadata.dtm.query.execution.core.exception.DtmException;
import io.arenadata.dtm.query.execution.core.service.schema.SystemDatamartViewsProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SystemDatamartViewsProviderImpl implements SystemDatamartViewsProvider {
    public static final String MNEMONIC = "information_schema";
    private final EntityDao entityDao;
    private List<SystemDatamartView> systemViews;
    private List<Datamart> datamartList;

    @Autowired
    public SystemDatamartViewsProviderImpl(ServiceDbFacade serviceDbFacade) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
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
            throw new DtmException("System Views is not loaded");
        }
    }

    @Override
    public Future<Void> fetchSystemViews() {
        //FIXME implement receiving system views
        return Future.succeededFuture();
    }

    private List<Datamart> getDatamartList(List<SystemDatamartView> systemViews) {
        val systemDatamart = new Datamart();
        systemDatamart.setEntities(systemViews.stream()
                .map(SystemDatamartView::getEntity)
                .collect(Collectors.toList()));
        systemDatamart.setMnemonic(MNEMONIC);
        return Collections.singletonList(systemDatamart);
    }

}
