package ru.ibs.dtm.common.service;

import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.service.impl.MetricRegistryServiceImpl;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

class MetricRegistryServiceTest {

  private MetricRegistryService service = new MetricRegistryServiceImpl();

  @Test
  void append() {
    String operation1 = "append1";
    String operation2 = "append2";
    service.append(operation1, 1000L);
    service.append(operation2, 2000L);
    assertThat(service.printSummaryStatistics(), containsString(String.format("%s;1;1", operation1)));
    assertThat(service.printSummaryStatistics(), containsString(String.format("%s;1;2", operation2)));
  }

  @Test
  void measureTimeMillis() {
    String operation = "operation1";
    service.measureTimeMillis(operation, () -> testBlock(1000L));
    service.measureTimeMillis(operation, () -> testBlock(1000L));
    assertThat(service.printSummaryStatistics(), containsString(String.format("%s;2;2", operation)));
  }

  private String testBlock(long millis) {
    CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return "complete";
    });
    return completableFuture.join();
  }



}
