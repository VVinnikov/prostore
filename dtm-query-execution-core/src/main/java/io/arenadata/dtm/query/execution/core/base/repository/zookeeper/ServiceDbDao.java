package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

public interface ServiceDbDao {

    DatamartDao getDatamartDao();

    EntityDao getEntityDao();

}
