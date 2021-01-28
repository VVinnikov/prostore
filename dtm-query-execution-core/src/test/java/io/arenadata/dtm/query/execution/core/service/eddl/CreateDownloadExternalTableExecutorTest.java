package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.exception.table.ExternalTableAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.exception.table.TableAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.service.avro.impl.AvroSchemaGeneratorImpl;
import io.arenadata.dtm.query.execution.core.service.eddl.impl.CreateDownloadExternalTableExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGeneratorImpl();
    private EddlExecutor createDownloadExteranlTableExecutor;
    private CreateDownloadExternalTableQuery query;
    private String schema;
    private Entity entity;
    private final Integer defaultChunkSize = 1000;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(edmlProperties.getDefaultChunkSize()).thenReturn(defaultChunkSize);
        createDownloadExteranlTableExecutor = new CreateDownloadExternalTableExecutor(serviceDbFacade, edmlProperties);

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
        query = CreateDownloadExternalTableQuery.builder()
                .schemaName(schema)
                .tableName(table)
                .entity(entity)
                .locationType(Type.KAFKA_TOPIC)
                .locationPath(locationPath)
                .format(Format.AVRO)
                .tableSchema(avroSchema.toString())
                .chunkSize(chunkSize)
                .build();

    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDefaultChunkSizeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        query.setChunkSize(null);
        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(query.getEntity().getExternalTableDownloadChunkSize(), defaultChunkSize);
    }

    @Test
    void executeDatamartNotExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof DatamartNotExistsException);
    }

    @Test
    void executeTableExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(true));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof ExternalTableAlreadyExistsException);
    }

    @Test
    void executeExistsDatamartError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture(new DtmException("exists datamart error")));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("exists datamart error", promise.future().cause().getMessage());
    }

    @Test
    void executeExistsEntityError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.failedFuture(new DtmException("exists entity error")));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("exists entity error", promise.future().cause().getMessage());
    }

    @Test
    void executeCreateEntityError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }
}
