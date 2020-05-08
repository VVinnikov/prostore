package ru.ibs.dtm.common.service;

/*DTO для квитанций об отправке от отправителя*/
public class SubscribeUploadRequest {
    private String groupId;
    private String topicName;

    public SubscribeUploadRequest() {
    }

    public SubscribeUploadRequest(String groupId, String topicName) {
        this.groupId = groupId;
        this.topicName = topicName;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
