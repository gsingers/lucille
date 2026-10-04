package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.kmwllc.lucille.core.CrawlConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.junit.Test;

public class KafkaCrawlerMessengerTest {

  private static final Config CONFIG = ConfigFactory.parseString("""
      kafka {
        bootstrapServers: "localhost:9092"
        consumerGroupId: "lucille_workers"
        maxPollIntervalSecs: 600
        maxRequestSize: 250000000
        consumer { max.poll.records: 50, enable.auto.commit: true }
      }
      """);

  @Test
  public void testWorkConsumerProps() {
    Properties props = KafkaCrawlerMessenger.createWorkConsumerProps(CONFIG, new CrawlConfig(CONFIG));

    // Crawlers form their own group, so that a Crawler joining or leaving does not disturb the Workers
    assertEquals("lucille_crawlers", props.get(ConsumerConfig.GROUP_ID_CONFIG));

    // Resolved by the ConsumerConfig of the kafka-clients jar on the classpath, not just read back from the Properties.
    ConsumerConfig resolved = new ConsumerConfig(props);

    // A unit must be claimed one at a time and confirmed only once it is finished, whatever kafka.consumer says.
    assertEquals(Integer.valueOf(1), resolved.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    assertEquals(false, resolved.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    assertEquals(List.of(CooperativeStickyAssignor.class.getName()),
        resolved.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
    assertEquals("earliest", resolved.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  @Test
  public void testWorkConsumerGroupIsConfigurable() {
    Config config = ConfigFactory.parseString("crawl.consumerGroupId: my_crawlers").withFallback(CONFIG);
    Properties props = KafkaCrawlerMessenger.createWorkConsumerProps(config, new CrawlConfig(config));

    assertEquals("my_crawlers", props.get(ConsumerConfig.GROUP_ID_CONFIG));
    assertNotEquals(config.getString("kafka.consumerGroupId"), props.get(ConsumerConfig.GROUP_ID_CONFIG));
  }
}
