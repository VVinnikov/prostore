package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper;

public interface ServiceDbDao {

    DatamartDao getDatamartDao();

    EntityDao getEntityDao();

}
