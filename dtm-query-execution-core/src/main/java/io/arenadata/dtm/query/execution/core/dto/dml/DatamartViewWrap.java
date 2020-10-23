package io.arenadata.dtm.query.execution.core.dto.dml;

import io.arenadata.dtm.query.execution.core.dto.DatamartView;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DatamartViewWrap {
    private final String datamart;
    private final DatamartView view;

    public DatamartViewPair getPair() {
        return new DatamartViewPair(datamart, view.getViewName().toLowerCase());
    }
}
