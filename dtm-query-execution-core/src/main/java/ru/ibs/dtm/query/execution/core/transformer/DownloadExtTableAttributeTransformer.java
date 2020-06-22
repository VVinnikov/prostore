package ru.ibs.dtm.query.execution.core.transformer;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.TableAttribute;
import ru.ibs.dtm.common.transformer.Transformer;
import ru.ibs.dtm.query.execution.core.dto.DownloadExternalTableAttribute;

@Component
public class DownloadExtTableAttributeTransformer implements Transformer<DownloadExternalTableAttribute, TableAttribute> {

    @Override
    public TableAttribute transform(DownloadExternalTableAttribute attribute) {
        return new TableAttribute(
                attribute.getColumnName(),
                attribute.getDataType(),
                attribute.getOrderNum()
        );
    }
}
