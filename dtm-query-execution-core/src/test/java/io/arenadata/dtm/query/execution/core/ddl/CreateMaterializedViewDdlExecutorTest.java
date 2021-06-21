package io.arenadata.dtm.query.execution.core.ddl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.InvalidSourceTypeException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.materializedview.MaterializedViewValidationException;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.MetadataCalciteGeneratorImpl;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.service.impl.CreateMaterializedViewDdlExecutor;
import io.arenadata.dtm.query.execution.core.dml.service.impl.ColumnMetadataServiceImpl;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.query.utils.DefaultDatamartSetter;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.assertException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CreateMaterializedViewDdlExecutorTest {
    private static final LimitSqlDialect SQL_DIALECT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);
    private static final String SCHEMA = "matviewdatamart";
    private static final String TBL_SCHEMA = "tbldatamart";
    private static final String TBL_ENTITY_NAME = "tbl";
    private static final String MAT_VIEW_ENTITY_NAME = "mat_view";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final FrameworkConfig frameworkConfig = DtmCalciteFramework.newConfigBuilder().parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final CoreCalciteSchemaFactory coreSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CalciteContextProvider contextProvider = new CoreCalciteContextProvider(parserConfig, coreSchemaFactory);

    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;
    @Mock
    private MetadataExecutor<DdlRequestContext> metadataExecutor;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DataSourcePluginService dataSourcePluginService;
    @Mock
    private CacheService<EntityKey, Entity> entityCacheService;
    @Mock
    private CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    @Mock
    private QueryParserService parserService;
    @InjectMocks
    private ColumnMetadataServiceImpl columnMetadataService;
    @InjectMocks
    private MetadataCalciteGeneratorImpl metadataCalciteGenerator;
    @InjectMocks
    private DefaultDatamartSetter defaultDatamartSetter;

    private QueryResultDdlExecutor createTableDdlExecutor;

    private Entity tblEntity;

    @BeforeEach
    void setUp() {
        lenient().when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        lenient().when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        lenient().when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        Set<SourceType> sourceTypes = new HashSet<>();
        sourceTypes.add(SourceType.ADB);
        sourceTypes.add(SourceType.ADG);
        lenient().when(dataSourcePluginService.getSourceTypes()).thenReturn(sourceTypes);
        lenient().when(dataSourcePluginService.hasSourceType(Mockito.any(SourceType.class))).thenAnswer(invocationOnMock -> sourceTypes.contains(invocationOnMock.getArgument(0, SourceType.class)));
        createTableDdlExecutor = new CreateMaterializedViewDdlExecutor(metadataExecutor, serviceDbFacade, new SqlDialect(SqlDialect.EMPTY_CONTEXT), entityCacheService,
                materializedViewCacheService, logicalSchemaProvider, columnMetadataService, parserService, metadataCalciteGenerator, dataSourcePluginService);

        tblEntity = Entity.builder()
                .name(TBL_ENTITY_NAME)
                .schema(SCHEMA)
                .entityType(EntityType.TABLE)
                .fields(Arrays.asList(
                        EntityField.builder()
                                .ordinalPosition(0)
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .primaryOrder(1)
                                .shardingOrder(1)
                                .build(),
                        EntityField.builder()
                                .ordinalPosition(1)
                                .name("name")
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .size(100)
                                .build(),
                        EntityField.builder()
                                .ordinalPosition(1)
                                .name("enddate")
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .accuracy(5)
                                .build()
                ))
                .build();

        Datamart tblDatamart = new Datamart(TBL_SCHEMA, false, Arrays.asList(tblEntity));
        Datamart mainDatamart = new Datamart(SCHEMA, true, Collections.emptyList());
        List<Datamart> logicSchema = Arrays.asList(mainDatamart, tblDatamart);
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), anyString())).thenReturn(Future.succeededFuture(logicSchema));
        lenient().when(parserService.parse(Mockito.any())).thenAnswer(invocationOnMock -> Future.succeededFuture(parse(invocationOnMock.getArgument(0, QueryParserRequest.class))));
    }

    @Test
    void shouldSuccessWhenStarQuery() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");
        context.setEntity(tblEntity);

        Promise<QueryResult> promise = Promise.promise();

        when(datamartDao.existsDatamart(SCHEMA))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(SCHEMA, MAT_VIEW_ENTITY_NAME))
                .thenReturn(Future.succeededFuture(false));
        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        if (promise.future().cause() != null) {
            fail(promise.future().cause());
        }
        verify(materializedViewCacheService).put(any(EntityKey.class), any(MaterializedViewCacheValue.class));
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }

    @Test
    void shouldSuccessWhenStarQueryAndAllTypes() {
        // arrange
        ArrayList<EntityField> fields = new ArrayList<>();
        fields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build());

        int pos = 1;
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.ANY || columnType == ColumnType.BLOB) continue;

            EntityField field = EntityField.builder()
                    .ordinalPosition(pos++)
                    .name("col_" + columnType.name().toLowerCase())
                    .type(columnType)
                    .nullable(true)
                    .build();

            switch (columnType) {
                case TIME:
                case TIMESTAMP:
                    field.setAccuracy(5);
                    break;
                case CHAR:
                case VARCHAR:
                    field.setSize(100);
                    break;
                case UUID:
                    field.setSize(36);
                    break;
            }

            fields.add(field);
        }

        tblEntity = Entity.builder()
                .name(TBL_ENTITY_NAME)
                .schema(SCHEMA)
                .entityType(EntityType.TABLE)
                .fields(fields)
                .build();

        Datamart tblDatamart = new Datamart(TBL_SCHEMA, false, Arrays.asList(tblEntity));
        List<Datamart> logicSchema = Arrays.asList(tblDatamart);
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), anyString())).thenReturn(Future.succeededFuture(logicSchema));

        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint,\n" +
                "        col_varchar varchar(100),\n" +
                "        col_char char(100),\n" +
                "        col_bigint bigint,\n" +
                "        col_int int,\n" +
                "        col_int32 int32,\n" +
                "        col_double double,\n" +
                "        col_float float,\n" +
                "        col_date date,\n" +
                "        col_time time(5),\n" +
                "        col_timestamp timestamp(5),\n" +
                "        col_boolean boolean,\n" +
                "        col_uuid uuid,\n" +
                "        col_link link,\n" +
                "        PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");

        Promise<QueryResult> promise = Promise.promise();

        when(datamartDao.existsDatamart(eq(SCHEMA)))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(eq(SCHEMA), eq(MAT_VIEW_ENTITY_NAME)))
                .thenReturn(Future.succeededFuture(false));
        when(entityDao.getEntity(eq(TBL_SCHEMA), eq(tblEntity.getName())))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        if (promise.future().cause() != null) {
            fail(promise.future().cause());
        }
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }

    @Test
    void shouldSuccessWhenExplicitQuery() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT id, name FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");
        context.setEntity(tblEntity);

        Promise<QueryResult> promise = Promise.promise();

        when(datamartDao.existsDatamart(SCHEMA))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(SCHEMA, MAT_VIEW_ENTITY_NAME))
                .thenReturn(Future.succeededFuture(false));
        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(metadataExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        if (promise.future().cause() != null) {
            fail(promise.future().cause());
        }
        verify(materializedViewCacheService).put(any(EntityKey.class), any(MaterializedViewCacheValue.class));
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }

    @Test
    void shouldFailWhenNoQuerySourceType() {
        testFailDatasourceType("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl",
                "DATASOURCE_TYPE not specified or invalid");
    }

    @Test
    void shouldFailWhenInvalidQuerySourceType() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'DB'");

        Promise<QueryResult> promise = Promise.promise();

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verifyNoInteractions(parserService, datamartDao, entityDao, materializedViewCacheService);
        assertTrue(promise.future().failed());
        assertException(InvalidSourceTypeException.class, "isn't a valid datasource type, please use one of the following:", promise.future().cause());
    }

    @Test
    void shouldFailWhenDisabledQuerySourceType() {
        testFailDatasourceType("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADQM'",
                "DATASOURCE_TYPE not specified or invalid");
    }

    @Test
    void shouldFailWhenDisabledDestination() {
        testFailDatasourceType("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADQM) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "DATASOURCE_TYPE has non exist items:");
    }

    @Test
    void shouldFailWhenForSystemTimePresentInQuery() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl FOR SYSTEM_TIME AS OF 1 DATASOURCE_TYPE = 'ADB'");

        Promise<QueryResult> promise = Promise.promise();

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verifyNoInteractions(parserService, datamartDao, entityDao, materializedViewCacheService);
        assertTrue(promise.future().failed());
        assertException(ViewDisalowedOrDirectiveException.class, "Disallowed view or directive in a subquery", promise.future().cause());
    }

    @Test
    void shouldFailWhenTblEntityIsNotLogicTable() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");

        Set<EntityType> allowedEntityTypes = EnumSet.of(EntityType.TABLE); // change this if something added

        Set<EntityType> disallowedEntityTypes = Arrays.stream(EntityType.values())
                .filter(entityType -> !allowedEntityTypes.contains(entityType))
                .collect(Collectors.toSet());

        for (EntityType entityType : disallowedEntityTypes) {
            // arrange 2
            reset(entityDao);
            when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                    .thenReturn(Future.succeededFuture(tblEntity));
            tblEntity.setEntityType(entityType);
            Promise<QueryResult> promise = Promise.promise();

            // act
            createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                    .onComplete(promise);

            // assert
            verify(entityDao).getEntity(TBL_SCHEMA, TBL_ENTITY_NAME);
            verifyNoMoreInteractions(entityDao);
            verifyNoInteractions(parserService);
            verifyNoInteractions(datamartDao);
            verifyNoInteractions(materializedViewCacheService);
            assertTrue(promise.future().failed());
            assertException(ViewDisalowedOrDirectiveException.class, "Disallowed view or directive in a subquery", promise.future().cause());
        }
    }

    @Test
    void shouldFailWhenNoPrimaryKey() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "Primary keys and Sharding keys are required",
                ValidationDtmException.class);
    }

    @Test
    void shouldFailWhenNoShardingKey() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "Primary keys and Sharding keys are required",
                ValidationDtmException.class);
    }

    @Test
    void shouldFailWhenCharColumnHasNoSize() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name char, enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "Specifying the size for columns[name] with types[CHAR] is required",
                ValidationDtmException.class);
    }

    @Test
    void shouldFailWhenQueryColumnsCountDifferWithView() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), num float, PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "has conflict with query columns wrong count",
                MaterializedViewValidationException.class);
    }

    @Test
    void shouldFailWhenQueryColumnsTypeNotMatch() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name char(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "has conflict with query types not equal for",
                MaterializedViewValidationException.class);
    }

    @Test
    void shouldFailWhenQueryColumnsSizeNotMatch() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar, enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "has conflict with query columns type size not equal for",
                MaterializedViewValidationException.class);
    }

    @Test
    void shouldFailWhenQueryColumnsPrecisionNotMatch() {
        testFailOnValidation("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp, PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'",
                "has conflict with query columns type accuracy not equal for",
                MaterializedViewValidationException.class);
    }

    @Test
    void shouldFailWhenDatamartException() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");

        Promise<QueryResult> promise = Promise.promise();

        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(datamartDao.existsDatamart(SCHEMA))
                .thenReturn(Future.failedFuture(new DatamartNotExistsException(SCHEMA)));

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verify(entityDao).getEntity(TBL_SCHEMA, tblEntity.getName());
        verifyNoMoreInteractions(entityDao);
        verifyNoInteractions(metadataExecutor, materializedViewCacheService);

        assertTrue(promise.future().failed());
        assertException(DatamartNotExistsException.class, "Database " + SCHEMA + " does not exist", promise.future().cause());
    }

    @Test
    void shouldFailWhenDatamartNotExist() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");

        Promise<QueryResult> promise = Promise.promise();

        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(datamartDao.existsDatamart(SCHEMA))
                .thenReturn(Future.succeededFuture(false));

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verify(entityDao).getEntity(TBL_SCHEMA, tblEntity.getName());
        verifyNoMoreInteractions(entityDao);
        verifyNoInteractions(metadataExecutor, materializedViewCacheService);

        assertTrue(promise.future().failed());
        assertException(DatamartNotExistsException.class, "Database " + SCHEMA + " does not exist", promise.future().cause());
    }

    @Test
    void shouldFailWhenEntityAlreadyExist() {
        // arrange
        DdlRequestContext context = getContext("CREATE MATERIALIZED VIEW mat_view (id bigint, name varchar(100), enddate timestamp(5), PRIMARY KEY(id))\n" +
                "DISTRIBUTED BY (id) DATASOURCE_TYPE (ADG) AS SELECT * FROM tbldatamart.tbl DATASOURCE_TYPE = 'ADB'");

        Promise<QueryResult> promise = Promise.promise();

        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));
        when(datamartDao.existsDatamart(SCHEMA))
                .thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(SCHEMA, MAT_VIEW_ENTITY_NAME)).thenReturn(Future.succeededFuture(true));

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verify(entityDao).getEntity(TBL_SCHEMA, tblEntity.getName());
        verifyNoMoreInteractions(entityDao);
        verifyNoInteractions(metadataExecutor, materializedViewCacheService);

        assertTrue(promise.future().failed());
        assertException(EntityAlreadyExistsException.class, "Entity " + MAT_VIEW_ENTITY_NAME + " already exists", promise.future().cause());
    }

    private void testFailDatasourceType(String sql, String errorMessage) {
        // arrange
        DdlRequestContext context = getContext(sql);

        Promise<QueryResult> promise = Promise.promise();

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verifyNoInteractions(parserService, datamartDao, entityDao, materializedViewCacheService);
        assertTrue(promise.future().failed());
        assertException(MaterializedViewValidationException.class, errorMessage, promise.future().cause());
    }

    private void testFailOnValidation(String sql, String errorMessage, Class<? extends Exception> exceptionClass) {
        // arrange
        DdlRequestContext context = getContext(sql);


        Promise<QueryResult> promise = Promise.promise();

        when(entityDao.getEntity(TBL_SCHEMA, tblEntity.getName()))
                .thenReturn(Future.succeededFuture(tblEntity));

        // act
        createTableDdlExecutor.execute(context, MAT_VIEW_ENTITY_NAME)
                .onComplete(promise);

        // assert
        verifyNoInteractions(datamartDao);
        verify(entityDao).getEntity(TBL_SCHEMA, tblEntity.getName());
        verifyNoMoreInteractions(entityDao);
        verifyNoInteractions(metadataExecutor, materializedViewCacheService);

        assertTrue(promise.future().failed());
        assertException(exceptionClass, errorMessage, promise.future().cause());
    }

    @SneakyThrows
    private DdlRequestContext getContext(String sql) {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(SCHEMA);
        queryRequest.setSql(sql);
        SqlNode sqlNode = planner.parse(queryRequest.getSql());
        defaultDatamartSetter.set(sqlNode, SCHEMA);
        DdlRequestContext context = new DdlRequestContext(null, new DatamartRequest(queryRequest), sqlNode, null, null);
        context.setDatamartName(SCHEMA);
        return context;
    }

    @SneakyThrows
    private QueryParserResponse parse(QueryParserRequest request) {
        val context = contextProvider.context(request.getSchema());
        val sql = request.getQuery().toSqlString(SQL_DIALECT).getSql();
        val parse = context.getPlanner().parse(sql);
        val validatedQuery = context.getPlanner().validate(parse);
        val relQuery = context.getPlanner().rel(validatedQuery);
        return new QueryParserResponse(
                context,
                request.getSchema(),
                relQuery,
                validatedQuery);
    }
}