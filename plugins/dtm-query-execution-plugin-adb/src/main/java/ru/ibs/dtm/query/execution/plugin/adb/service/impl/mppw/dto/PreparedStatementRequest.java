package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import io.reactiverse.pgclient.Tuple;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PreparedStatementRequest {
    private String sql;
    private Tuple params;
}
