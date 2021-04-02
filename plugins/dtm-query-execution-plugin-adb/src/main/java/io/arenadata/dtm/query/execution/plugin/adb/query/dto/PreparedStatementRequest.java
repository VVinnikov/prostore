package io.arenadata.dtm.query.execution.plugin.adb.query.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PreparedStatementRequest {
    private String sql;
    private List<Object> params;

    public static PreparedStatementRequest onlySql(String sql) {
        return new PreparedStatementRequest(sql, Collections.emptyList());
    }
}
