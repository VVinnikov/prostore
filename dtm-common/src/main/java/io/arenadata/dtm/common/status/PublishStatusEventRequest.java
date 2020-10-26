package io.arenadata.dtm.common.status;

import lombok.Data;

@Data
public class PublishStatusEventRequest<T> {
    private final StatusEventKey eventKey;
    private final T eventMessage;
}
