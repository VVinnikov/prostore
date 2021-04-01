package io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ServiceDbDaoImpl implements ServiceDbDao {
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    @Override
    public DatamartDao getDatamartDao() {
        return datamartDao;
    }

    @Override
    public EntityDao getEntityDao() {
        return entityDao;
    }
}
