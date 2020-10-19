package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.TarantoolDatabaseProperties;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TtCartridgeSchemaGeneratorImplTest {

    private TtCartridgeSchemaGenerator cartridgeSchemaGenerator;
    private ObjectMapper mapper;
    private DdlRequestContext ddlRequestContext;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setEnvName("test");
        queryRequest.setSourceType(SourceType.ADG);
        List<EntityField> fields = Collections.singletonList(new EntityField(0,"test_field", "varchar(1)", false, ""));
        Entity entity = new Entity("test_schema.test_table", fields);

        ddlRequestContext = new DdlRequestContext(new DdlRequest(queryRequest, entity));
    }

    @Test
    void generateWithEmptySpaces() throws JsonProcessingException {
        Promise promise = Promise.promise();
        cartridgeSchemaGenerator = new TtCartridgeSchemaGeneratorImpl(new TarantoolDatabaseProperties());
        cartridgeSchemaGenerator.generate(ddlRequestContext, mapper.readValue("{}", OperationYaml.class), ar -> {
            if (ar.succeeded()){
                promise.complete(ar.result());

            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
    }
}