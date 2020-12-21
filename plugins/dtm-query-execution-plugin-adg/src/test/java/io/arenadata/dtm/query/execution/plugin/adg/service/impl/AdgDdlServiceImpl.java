package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.configuration.kafka.KafkaAdminProperty;
import io.arenadata.dtm.common.configuration.kafka.KafkaConfig;
import io.arenadata.dtm.common.configuration.kafka.KafkaUploadProperty;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeProvider;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl.AdgDdlService;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;
import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.DROP_TABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AdgDdlServiceImpl {

    private AdgCartridgeProvider cartridgeProvider = mock(AdgCartridgeProvider.class);
    private final KafkaConfig kafkaProperties = mock(KafkaConfig.class);
    private final QueryExecutorService executorService = mock(QueryExecutorService.class);

    private final AdgDdlService adgDdlService = new AdgDdlService();

    @Test
    @Disabled
    void testExecuteNotEmptyOk() {
        KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
        KafkaUploadProperty kafkaUploadProperty = new KafkaUploadProperty();
        Map<String, String> rq = new HashMap<>();
        Map<String, String> rs = new HashMap<>();
        Map<String, String> err = new HashMap<>();
        rq.put(SourceType.ADG.toString().toLowerCase(), "%s.%s.adg.upload.rq");
        rs.put(SourceType.ADG.toString().toLowerCase(), "%s.%s.adg.upload.rs");
        err.put(SourceType.ADG.toString().toLowerCase(), "%s.%s.adg.upload.err");

        kafkaUploadProperty.setRequestTopic(rq);
        kafkaUploadProperty.setResponseTopic(rs);
        kafkaUploadProperty.setErrorTopic(err);
        kafkaAdminProperty.setUpload(kafkaUploadProperty);
        when(kafkaProperties.getKafkaAdminProperty()).thenReturn(kafkaAdminProperty);

        doAnswer(invocation -> {
            Handler<AsyncResult<Object>> handler = invocation.getArgument(0);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(executorService).executeProcedure(eq(DROP_SPACE), eq("test_table"));

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("drop table test_table");
        queryRequest.setDatamartMnemonic("test_schema");

        List<EntityField> fields = Collections.singletonList(new EntityField(0,"test_field", "varchar(1)", false, ""));
        Entity entity = new Entity("test_schema.test_table", fields);

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(queryRequest, entity));
        context.setDdlType(DROP_TABLE);
        adgDdlService.execute(context);
    }
}
