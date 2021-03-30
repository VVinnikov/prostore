package io.arenadata.dtm.query.execution.core.transformer;

import io.arenadata.dtm.common.plugin.exload.TableAttribute;
import io.arenadata.dtm.common.transformer.Transformer;
import io.arenadata.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;
import org.springframework.stereotype.Component;

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
