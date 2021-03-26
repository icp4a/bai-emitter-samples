/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.iaf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class IAFConsumer {

  public IAFConsumer(Properties consumerProperties, String topic) {
    // Set the consumer group ID in the properties
    consumerProperties.putIfAbsent("group.id", "icp4ba-bai");

    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Get a new KafkaConsumer (a connection to the schema registry is performed)
    try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProperties)) {

      // Subscribe to the topic...
      consumer.subscribe(Arrays.asList(topic));

      System.out.println("Kafka consumer listening for messages on topic '" + topic + "'");
      // Poll the topic to retrieve records
      int maxRetry = 24;
      int retry = 0;
      int durationSeconds = 5;
      boolean recordFound = false;
      int messages = 0;
      while (retry < maxRetry) {
        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(durationSeconds));

        records.forEach(rec -> System.out.printf("Received a message: offset: %d, partition: %d, key: %s, value: %s\n",
            rec.partition(), rec.offset(), rec.key(), rec.value()));
        retry++;
        // stopping at first non empty record...
        if ( ! records.isEmpty()) {
          recordFound = true;
          messages = records.count();
          break;
        }
      }
      if ( ! recordFound) {
        System.out.println("No record found after " + (retry * durationSeconds) + " seconds.");
      } else {
        System.out.println("Found " + messages + " message(s).");
      }
    }
  }

}
