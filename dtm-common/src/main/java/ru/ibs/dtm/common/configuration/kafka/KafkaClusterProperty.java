package ru.ibs.dtm.common.configuration.kafka;

public class KafkaClusterProperty {
  String zookeeperHosts = "";
  String rootPath = "";

  public String getZookeeperHosts() {
    return zookeeperHosts;
  }

  public void setZookeeperHosts(String zookeeperHosts) {
    this.zookeeperHosts = zookeeperHosts;
  }

  public String getRootPath() {
    return rootPath;
  }

  public void setRootPath(String rootPath) {
    this.rootPath = rootPath;
  }
}
