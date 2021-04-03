package io.arenadata.dtm.query.execution.plugin.adqm.base.configuration.datasource;

import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.Properties;

public class AdqmBalancedClickhouseDataSource extends BalancedClickhouseDataSource {
    public AdqmBalancedClickhouseDataSource(String url) {
        super(url);
    }

    public AdqmBalancedClickhouseDataSource(String url, Properties properties) {
        super(url, properties);
    }

    public AdqmBalancedClickhouseDataSource(String url, ClickHouseProperties properties) {
        super(url, properties);
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        try {
            return super.getConnection();
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataSourceException(ex);
        }
    }

    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        try {
            return super.getConnection(username, password);
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataSourceException(ex);
        }
    }
}
