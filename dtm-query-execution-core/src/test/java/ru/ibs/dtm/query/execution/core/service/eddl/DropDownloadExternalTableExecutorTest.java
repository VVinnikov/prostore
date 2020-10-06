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
import ru.ibs.dtm.query.execution.core.dto.eddl.DropDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.service.eddl.impl.DropDownloadExternalTableExecutor;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private EddlExecutor dropDownloadExternalTableExecutor;
    private DropDownloadExternalTableQuery query;
    private String schema;
    private String table;
    private Entity entity;
    private Entity entityWithWrongType;

    @BeforeEach
    void setUp(){
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropDownloadExternalTableExecutor = new DropDownloadExternalTableExecutor(serviceDbFacade);
        schema = "shares";
        table = "accounts";
        query = new DropDownloadExternalTableQuery(schema, table);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);

        entityWithWrongType = new Entity(table, schema, Arrays.asList(f1, f2));
        entityWithWrongType.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
    }

    @Test
    void executeSuccess(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture());

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeTableExistsWithWrongType(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entityWithWrongType));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Table [%s] in datamart [%s] doesn't exist!", table, schema), promise.future().cause().getMessage());
    }

    @Test
    void executeTableNotExists(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("entity not exists"));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("entity not exists", promise.future().cause().getMessage());
    }

    @Test
    void executeDeleteEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("delete entity error"));

        dropDownloadExternalTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("delete entity error", promise.future().cause().getMessage());
    }
}
