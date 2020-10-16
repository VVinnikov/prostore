package ru.ibs.dtm.query.execution.core.service.impl;

import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import ru.ibs.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class InformationSchemaExecutorImplTest {

    private ServiceDbFacade serviceDbFacade = mock(ServiceDbFacade.class);
    private InformationSchemaExecutor informationSchemaExecutor = new InformationSchemaExecutorImpl(serviceDbFacade);

    @Test
    void executeQuery() {
        //FIXME
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("schema_name", ColumnType.VARCHAR));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("schema_name", "test_datamart");
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select * from \"INFORMATION_SCHEMA\".schemata");

        sourceRequest.setQueryRequest(queryRequest);
        sourceRequest.setMetadata(metadata);
        sourceRequest.setSourceType(SourceType.INFORMATION_SCHEMA);

        informationSchemaExecutor.execute(sourceRequest, ar -> {
            assertTrue(ar.succeeded());
            Map<String, Object> expectedMap = new HashMap<>();
            expectedMap.put("schema_name", "test_datamart");
            assertEquals(expectedMap, ar.result().getResult().get(0));
        });
    }
}
