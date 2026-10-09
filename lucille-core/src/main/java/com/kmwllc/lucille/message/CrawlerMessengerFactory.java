package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.CrawlConfig;
import com.typesafe.config.Config;

public interface CrawlerMessengerFactory {

  CrawlerMessenger create();

  /**
   * Releases anything shared by the messengers this factory created. The messengers themselves are closed by the
   * Crawlers that use them.
   */
  default void close() {
  }

  static CrawlerMessengerFactory getConstantFactory(CrawlerMessenger messenger) {
    return () -> messenger;
  }

  /**
   * Returns a factory whose messengers each have their own Kafka clients but share one view of which runs are alive.
   */
  static CrawlerMessengerFactory getKafkaFactory(Config config) {
    CrawlConfig crawlConfig = new CrawlConfig(config);
    KafkaRunControlListener listener = new KafkaRunControlListener(config, crawlConfig);
    listener.start();

    return new CrawlerMessengerFactory() {
      @Override
      public CrawlerMessenger create() {
        return new KafkaCrawlerMessenger(config, crawlConfig, listener.getTracker());
      }

      @Override
      public void close() {
        listener.stop();
      }
    };
  }
}
