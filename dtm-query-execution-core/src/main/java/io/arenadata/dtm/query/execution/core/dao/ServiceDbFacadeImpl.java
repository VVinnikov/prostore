package io.arenadata.dtm.query.execution.core.dao;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ServiceDbFacadeImpl implements ServiceDbFacade {

    private final ServiceDbDao serviceDbDao;
    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public ServiceDbFacadeImpl(ServiceDbDao serviceDbDao, DeltaServiceDao deltaServiceDao) {
        this.serviceDbDao = serviceDbDao;
        this.deltaServiceDao = deltaServiceDao;
    }

    @Override
    public ServiceDbDao getServiceDbDao() {
        return serviceDbDao;
    }

    @Override
    public DeltaServiceDao getDeltaServiceDao() {
        return deltaServiceDao;
    }
}
