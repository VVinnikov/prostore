package io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class AdgSynchronizeSqlFactoryTest {

    public static final String TARANTOOL_SERVER = "tarantool_server";
    public static final String ENV = "env";
    public static final String DATAMART = "datamart";
    public static final String ENTITY_NAME = "entity_name";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final long CONNECT_TIMEOUT = 1234L;
    public static final long READ_TIMEOUT = 2345L;
    public static final long REQUEST_TIMEOUT = 3456L;
    public static final String QUERY = "query";
    @Mock
    private AdgSharedService adgSharedService;

    @InjectMocks
    private AdgSynchronizeSqlFactory adgSynchronizeSqlFactory;

    @BeforeEach
    void setUp() {
        lenient().when(adgSharedService.getSharedProperties()).thenReturn(new AdgSharedProperties(TARANTOOL_SERVER, USER, PASSWORD, CONNECT_TIMEOUT, READ_TIMEOUT, REQUEST_TIMEOUT));
    }

    @Test
    void shouldPrepareCreateExternalTable() {
        // arrange
        Entity build = getEntity();

        // act
        String sql = adgSynchronizeSqlFactory.createExternalTable(ENV, DATAMART, build);

        // assert
        Assertions.assertThat(sql).isEqualToNormalizingNewlines("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_entity_name\n" +
                "(col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date int8,col_time int8,col_timestamp int8,col_boolean bool,col_uuid varchar,col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart__entity_name_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456')\n" +
                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
    }

    @Test
    void shouldPrepareDropExternalTable() {
        // arrange
        Entity entity = getEntity();

        // act
        String sql = adgSynchronizeSqlFactory.dropExternalTable(DATAMART, entity);

        // assert
        assertEquals("DROP EXTERNAL TABLE IF EXISTS " + DATAMART + ".TARANTOOL_EXT_" + ENTITY_NAME, sql);
    }

    @Test
    void shouldPrepareInsertIntoExternalTable() {
        // arrange
        Entity entity = getEntity();

        // act
        String sql = adgSynchronizeSqlFactory.insertIntoExternalTable(DATAMART, entity, QUERY);

        // assert
        assertEquals("INSERT INTO " + DATAMART + ".TARANTOOL_EXT_" + ENTITY_NAME + " " + QUERY, sql);
    }

    private Entity getEntity() {
        List<EntityField> fields = new ArrayList<>();
        int pos = 0;
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

        return Entity.builder()
                .name(ENTITY_NAME)
                .fields(fields)
                .build();
    }
}