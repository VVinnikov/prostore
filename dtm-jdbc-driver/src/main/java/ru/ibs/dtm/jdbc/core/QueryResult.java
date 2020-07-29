package ru.ibs.dtm.jdbc.core;

import java.util.List;
import java.util.Map;
import lombok.Data;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

/**
 * Результат выполнения sql-команды
 */
@Data
public class QueryResult {
    /**
     * Идентификатор запроса
     */
    private String requestId;

    /**
     * Список строк ответа sql-команды
     */
    private List<Map<String, Object>> result;

    /**
     * Признак пустого запроса
     */
    private boolean empty;

    private List<ColumnMetadata> metadata;
}
