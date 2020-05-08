package ru.ibs.dtm.common.newdata;

/*DTO цельного сообщения по шине*/
public class DataMessage {
    DataMessageRequestKey key;
    DataMessageRequestValue value;

    public DataMessage() {
    }

    public DataMessage(DataMessageRequestKey key, DataMessageRequestValue value) {
        this.key = key;
        this.value = value;
    }

    public DataMessageRequestKey getKey() {
        return key;
    }

    public void setKey(DataMessageRequestKey key) {
        this.key = key;
    }

    public DataMessageRequestValue getValue() {
        return value;
    }

    public void setValue(DataMessageRequestValue value) {
        this.value = value;
    }
}
