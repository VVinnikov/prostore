package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import ru.ibs.dtm.query.execution.core.service.avro.AvroSchemaGeneratorImpl;
import ru.ibs.dtm.query.execution.core.service.eddl.impl.CreateDownloadExternalTableExecutor;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGeneratorImpl();
    private EddlExecutor createDownloadExteranlTableExecutor;
    private CreateDownloadExternalTableQuery query;
    private String schema;
    private Entity entity;

    @BeforeEach
    void setUp(){
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        createDownloadExteranlTableExecutor = new CreateDownloadExternalTableExecutor(serviceDbFacade);

        schema = "shares";
        String table = "accounts";
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
        Schema avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
        int chunkSize = 10;
        String locationPath = "kafka://localhost:2181/KAFKA_TOPIC";
        query = new CreateDownloadExternalTableQuery(schema,
                table,
                entity,
                Type.KAFKA_TOPIC,
                locationPath,
                Format.AVRO,
                avroSchema.toString(),
                chunkSize);

    }

    @Test
    void executeSuccess(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDatamartNotExists(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Datamart [%s] not exists!", schema), promise.future().cause().getMessage());
    }

    @Test
    void executeTableExists(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(true));

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals(String.format("Table [%s] is already exists in datamart [%s]!", entity.getName(), schema), promise.future().cause().getMessage());
    }

    @Test
    void executeExistsDatamartError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture("exists datamart error"));

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("exists datamart error", promise.future().cause().getMessage());
    }

    @Test
    void executeExistsEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.failedFuture("exists entity error"));

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeCreateEntityError(){
        Promise promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture("create entity error"));

        createDownloadExteranlTableExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }
}
