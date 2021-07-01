package io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.SynchronizeSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.utils.AdbTypeUtil.adbTypeFromDtmType;

@Service
public class AdgSynchronizeSqlFactory implements SynchronizeSqlFactory {
    private static final String DROP_EXTERNAL_TABLE = "DROP EXTERNAL TABLE IF EXISTS %s.TARANTOOL_EXT_%s";
    private static final String CREATE_EXTERNAL_TABLE = "CREATE WRITABLE EXTERNAL TABLE %s.TARANTOOL_EXT_%s\n" +
            "(%s) LOCATION ('pxf://%s?PROFILE=tarantool-upsert&TARANTOOL_SERVER=%s&USER=%s&PASSWORD=%s&TIMEOUT_CONNECT=%d&TIMEOUT_READ=%d&TIMEOUT_REQUEST=%d')\n" +
            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')";
    private static final String INSERT_INTO_EXTERNAL_TABLE = "INSERT INTO %s.TARANTOOL_EXT_%s %s";
    private final AdgSharedService adgSharedService;

    public AdgSynchronizeSqlFactory(AdgSharedService adgSharedService) {
        this.adgSharedService = adgSharedService;
    }

    @Override
    public String createExternalTable(String env, String datamart, Entity matView) {
        val spaceName = getSpaceName(env, datamart, matView);
        val columns = getColumns(matView);
        val sharedProperties = adgSharedService.getSharedProperties();
        return String.format(CREATE_EXTERNAL_TABLE, datamart, matView.getName(),
                columns, spaceName, sharedProperties.getServer(), sharedProperties.getUser(), sharedProperties.getPassword(),
                sharedProperties.getConnectTimeout(), sharedProperties.getReadTimeout(), sharedProperties.getRequestTimeout());
    }

    @Override
    public String dropExternalTable(String datamart, Entity matView) {
        return String.format(DROP_EXTERNAL_TABLE, datamart, matView.getName());
    }

    @Override
    public String insertIntoExternalTable(String datamart, Entity matView, String query) {
        return String.format(INSERT_INTO_EXTERNAL_TABLE, datamart, matView.getName(), query);
    }

    private String getSpaceName(String env, String datamart, Entity matView) {
        return String.format("%s__%s__%s_staging", env, datamart, matView.getName());
    }

    private String getColumns(Entity matView) {
        val builder = new StringBuilder();
        val fields = matView.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .collect(Collectors.toList());
        for (int i = 0; i < fields.size(); i++) {
            val field = fields.get(i);
            if (i > 0) {
                builder.append(',');
            }
            builder.append(field.getName()).append(' ').append(adbTypeFromDtmType(field.getType(), null));
        }

        builder.append(",sys_op ").append(adbTypeFromDtmType(ColumnType.BIGINT, null));
        builder.append(",bucket_id ").append(adbTypeFromDtmType(ColumnType.BIGINT, null));
        return builder.toString();
    }

}
