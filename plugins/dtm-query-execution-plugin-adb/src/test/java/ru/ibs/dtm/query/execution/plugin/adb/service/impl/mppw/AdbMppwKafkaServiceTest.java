package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdbMppwKafkaServiceTest {

    private RestLoadRequest restLoadRequest;
    private MppwTransferDataRequest mppwTransferDataRequest;
    private MppwKafkaRequestContext kafkaRequestContext;

    @Test
    void convertKafkaRequestContextTest() {
        restLoadRequest = RestLoadRequest.builder()
        .requestId(UUID.randomUUID().toString())
                .consumerGroup("test")
                .datamart("test")
                .tableName("tab_")
                .format("avro")
                .sysCn(3L)
                .kafkaTopic("test_topic")
                .messageProcessingLimit(1000)
                .zookeeperHost("zhost")
                .zookeeperPort(2181)
                .schema(new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"accounts_ext_dtm_536\",\"namespace\":\"dtm_536\",\"fields\":[{\"name\":\"account_id\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"account_type\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}"))
        .build();

        mppwTransferDataRequest = MppwTransferDataRequest.builder()
                .columnList(Arrays.asList("account_id", "account_type"))
                .keyColumnList(Arrays.asList("account_id"))
                .hotDelta(3L)
                .tableName("tab_")
                .datamart("test")
                .build();

        kafkaRequestContext = new MppwKafkaRequestContext(restLoadRequest, mppwTransferDataRequest);

        final String encodeRequest = Json.encode(kafkaRequestContext);

        final RestLoadRequest restLoadRequestDecoded =
                Json.decodeValue(((JsonObject) Json.decodeValue(encodeRequest))
                        .getJsonObject("restLoadRequest").toString(), RestLoadRequest.class);
        final MppwTransferDataRequest transferDataRequestDecoded =
                Json.decodeValue(((JsonObject) Json.decodeValue(encodeRequest))
                        .getJsonObject("mppwTransferDataRequest").toString(), MppwTransferDataRequest.class);
        assertEquals(restLoadRequest, restLoadRequestDecoded);
        assertEquals(mppwTransferDataRequest, transferDataRequestDecoded);
    }
}
