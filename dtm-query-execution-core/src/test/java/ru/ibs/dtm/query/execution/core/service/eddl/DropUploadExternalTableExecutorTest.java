package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.service.eddl.impl.DropUploadExternalTableExecutor;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropUploadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private EddlExecutor dropUploadExternalTableExecutor;
    private DropUploadExternalTableQuery query;
    private String schema;
    private String table;
    private Entity entity;
    private Entity entityWithWrongType;

    @BeforeEach
    void setUp(){
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropUploadExternalTableExecutor = new DropUploadExternalTableExecutor(serviceDbFacade);
        schema = "shares";
        table = "accounts";
        query = new DropUploadExternalTableQuery(schema, table);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);

        entityWithWrongType = new Entity(table, schema, Arrays.asList(f1, f2));
        entityWithWrongType.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
    }

    @Test
    void executeSuccess(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture());

        dropUploadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertFalse(promise.future().failed());
    }

    @Test
    void executeTableExistsWithWrongType(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entityWithWrongType));

        dropUploadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertFalse(promise.future().succeeded());
        assertTrue(promise.future().failed());
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeDeleteEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("deleting entity error"));

        dropUploadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertFalse(promise.future().succeeded());
        assertTrue(promise.future().failed());
        assertNotNull(promise.future().cause());
    }
}
