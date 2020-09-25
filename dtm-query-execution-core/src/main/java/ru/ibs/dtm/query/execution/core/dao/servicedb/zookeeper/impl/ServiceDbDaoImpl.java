package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;

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
