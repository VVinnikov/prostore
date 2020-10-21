package ru.ibs.dtm.query.execution.plugin.adqm.factory.impl;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class AdqmRollbackRequestFactoryTest {
    private static final List<String> EXPECTED_SQLS = Arrays.asList(
        "DROP TABLE env_1__dtm.tbl1_ext_shard ON CLUSTER cluster_1",
        "DROP TABLE env_1__dtm.tbl1_buffer_loader_shard ON CLUSTER cluster_1",
        "DROP TABLE env_1__dtm.tbl1_buffer ON CLUSTER cluster_1",
        "DROP TABLE env_1__dtm.tbl1_buffer_shard ON CLUSTER cluster_1",
        "SYSTEM FLUSH DISTRIBUTED env_1__dtm.tbl1_actual",
        "INSERT INTO env_1__dtm.tbl1_actual\n" +
            "  SELECT f1,f2,f3, sys_from, sys_to, sys_op, close_date, -1 AS sign\n" +
            "  FROM env_1__dtm.tbl1_actual\n" +
            "  WHERE sys_from = 11 AND sign = 1\n" +
            "  UNION ALL\n" +
            "  SELECT f1,f2,f3, sys_from, 9223372036854775807 AS sys_to, 0 AS sys_op, 9223372036854775807 AS close_date, arrayJoin(-1, 1) AS sign\n" +
            "  FROM env_1__dtm.tbl1_actual\n" +
            "  WHERE sys_to = 11 - 1 AND sign = 1",
        "SYSTEM FLUSH DISTRIBUTED env_1__dtm.tbl1_actual",
        "OPTIMIZE TABLE env_1__dtm.tbl1_actual_shard ON CLUSTER cluster_1 FINAL"
    );
    private final AdqmRollbackRequestFactory factory;
    private final Entity entity;

    public AdqmRollbackRequestFactoryTest() {
        this.entity = Entity.builder()
            .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
            .externalTableFormat("avro")
            .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
            .externalTableLocationType(ExternalTableLocationType.KAFKA)
            .externalTableUploadMessageLimit(1000)
            .name("tbl1")
            .schema("dtm")
            .fields(Arrays.asList(
                EntityField.builder()
                    .name("f1")
                    .build(),
                EntityField.builder()
                    .name("f2")
                    .build(),
                EntityField.builder()
                    .name("f3")
                    .build()
            ))
            .externalTableSchema("")
            .build();

        DdlProperties ddlProperties = new DdlProperties();
        ddlProperties.setCluster("cluster_1");
        factory = new AdqmRollbackRequestFactory(ddlProperties);
    }

    @Test
    void create() {
        val adqmRollbackRequest = factory.create(RollbackRequest.builder()
            .datamart("dtm")
            .entity(entity)
            .queryRequest(QueryRequest.builder()
                .envName("env_1")
                .build())
            .sysCn(11)
            .targetTable("tbl1")
            .build());
        log.info(adqmRollbackRequest.toString());
        assertEquals(EXPECTED_SQLS, adqmRollbackRequest.getStatements().stream()
            .map(PreparedStatementRequest::getSql)
            .collect(Collectors.toList()));
    }
}
