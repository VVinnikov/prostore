package io.arenadata.dtm.query.execution.plugin.adg.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("adg.tarantool.cartridge")
public class TarantoolCartridgeProperties {

  private String adminApiUrl = "/admin/api";

  private String sendQueryUrl = "/api/kafka/send_query";

  private String url;

  private String kafkaSubscriptionUrl = "/api/v1/kafka/subscription";

  private String kafkaLoadDataUrl = "/api/v1/kafka/dataload";

  private String transferDataToScdTableUrl = "/api/etl/transfer_data_to_scd_table";

  private String kafkaUploadDataUrl = "/api/v1/kafka/dataunload/query";

  private String tableBatchDeleteUrl = "/api/v1/ddl/table/batchDelete";

  private String tableQueuedCreate = "/api/v1/ddl/table/queuedCreate";

  private String tableQueuedDelete = "/api/v1/ddl/table/queuedDelete";

  private String reverseHistoryTransferUrl = "/api/v1/ddl/table/reverseHistoryTransfer";

  private String tableSchemaUrl = "/api/v1/ddl/table/schema";
}
