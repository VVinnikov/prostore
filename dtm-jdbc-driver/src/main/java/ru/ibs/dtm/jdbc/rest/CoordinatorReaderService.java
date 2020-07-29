package ru.ibs.dtm.jdbc.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ibs.dtm.jdbc.core.QueryRequest;
import ru.ibs.dtm.jdbc.core.QueryResult;
import ru.ibs.dtm.jdbc.model.ColumnInfo;
import ru.ibs.dtm.jdbc.model.SchemaInfo;
import ru.ibs.dtm.jdbc.model.TableInfo;
import ru.ibs.dtm.jdbc.util.DtmException;
import ru.ibs.dtm.jdbc.util.ResponseException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.http.util.TextUtils.isEmpty;
import static ru.ibs.dtm.jdbc.util.DriverConstants.HOST_PROPERTY;

/**
 * Контроллер взаимодействия с Сервисом чтения по REST
 */
public class CoordinatorReaderService implements Protocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorReaderService.class);
    private static final String GET_META_URL = "/meta";
    private static final String GET_ENTITIES_URL = "/meta/%s/entities";
    private static final String GET_ATTRIBUTES_URL = "/meta/%s/entity/%s/attributes";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CloseableHttpClient client;
    private final String backendHostUrl;
    private final String schema;

    @SneakyThrows
    public CoordinatorReaderService(CloseableHttpClient client, String dbHost, String schema) {
        if (isEmpty(dbHost)) {
            throw new DtmException(String.format("Невозможно создать подключение, потому что параметр '%s'не задан", HOST_PROPERTY));
        }
        this.backendHostUrl = "http://" + dbHost;
        this.schema = schema;
        this.client = client;
    }

    @Override
    public List<SchemaInfo> getDatabaseSchemas() {
        try {
            HttpGet httpGet = new HttpGet(backendHostUrl + GET_META_URL);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, new TypeReference<List<SchemaInfo>>() {
                });
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка при загрузке схем бд.", e.getCause());
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    @Override
    public List<TableInfo> getDatabaseTables(String schemaPattern) {
        try {
            String uri = String.format(backendHostUrl + GET_ENTITIES_URL, schemaPattern);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();

                return MAPPER.readValue(content, new TypeReference<List<TableInfo>>() {
                });
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка при загрузке таблиц схемы {}", schemaPattern, e.getCause());
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    @Override
    public List<ColumnInfo> getDatabaseColumns(String schema, String tableName) {

        try {
            String uri = String.format(backendHostUrl + GET_ATTRIBUTES_URL, schema, tableName);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();

                return MAPPER.readValue(content, new TypeReference<List<ColumnInfo>>() {
                });
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка при загрузке колонок таблицы {} схемы {}", tableName, schema, e.getCause());
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    @Override
    public QueryResult executeQuery(String sql) throws SQLException {
        try {
            HttpPost httpPost = new HttpPost(backendHostUrl + "/query/execute");
            QueryRequest queryRequest = prepareQueryRequest(sql);
            String queryRequestJson = MAPPER.writeValueAsString(queryRequest);
            LOGGER.debug("Подготовка запроса query [{}]", queryRequestJson);
            httpPost.setEntity(new StringEntity(queryRequestJson, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                QueryResult result = MAPPER.readValue(content, new TypeReference<QueryResult>() {
                });
                LOGGER.info("Получен ответ выполнения запроса {}", result);
                return result;
            }
        } catch (Exception e) {
            String errMsg = String.format("Ошибка при выполнении запроса [%s]: %s", sql, e.getMessage());
            LOGGER.error(errMsg, e);
            throw new SQLException(errMsg, e);
        }
    }

    @SneakyThrows
    private void checkResponseStatus(CloseableHttpResponse response) {
        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            try {
                String res = MAPPER.readValue(response.getEntity().getContent(), ResponseException.class)
                        .getExceptionMessage();
                LOGGER.error("Система вернула неуспешный ответ: {}", res);
                throw new DtmException(res != null && ! res.isEmpty() ? res :
                        String.format("Система вернула неуспешный ответ: %s", response.getStatusLine().getReasonPhrase()));
            } catch (DtmException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.error("Система вернула неуспешный ответ: {}", response.getStatusLine().getReasonPhrase());
                throw new DtmException(String.format("Система вернула неуспешный ответ: %s", response.getStatusLine().getReasonPhrase()));
            }
        }
    }

    private QueryRequest prepareQueryRequest(String sql) {
        UUID uuid = UUID.randomUUID();
        String schema = this.schema;

        QueryRequest queryRequest = new QueryRequest(uuid, schema, sql);
        LOGGER.info("Сформирован sql-запрос {}", queryRequest);
        return queryRequest;
    }
}
