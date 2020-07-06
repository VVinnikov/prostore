package ru.ibs.dtm.query.execution.core.dto.dml;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;

@Data
@AllArgsConstructor
public class DatamartViewWrap {
    private final String datamart;
    private final DatamartView view;

    public DatamartViewPair getPair() {
        return new DatamartViewPair(datamart, view.getViewName().toLowerCase());
    }
}
