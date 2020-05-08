package ru.ibs.dtm.common.eventbus;

import ru.ibs.dtm.common.newdata.DataMessage;

/*
 * Дто для передачи по шине с доп параметром deltaHot, чтобы не вмешиваться в мета данные
 * */
public class DataTempMessage extends DataMessage {

    Integer deltaHot;

    public DataTempMessage() {
    }

    public DataTempMessage(DataMessage dataMessage, Integer deltaHot) {
        this.setKey(dataMessage.getKey());
        this.setValue(dataMessage.getValue());
        this.setDeltaHot(deltaHot);
    }

    public Integer getDeltaHot() {
        return deltaHot;
    }

    public void setDeltaHot(Integer deltaHot) {
        this.deltaHot = deltaHot;
    }
}
