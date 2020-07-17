/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.eventstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EventStreamConsumer {
  public EventStreamConsumer(Properties consumerProperties, String topic) {
    // Set the consumer group ID in the properties
    consumerProperties.putIfAbsent("group.id", "consumer" + Math.random());

    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Get a new KafkaConsumer (a connection to the schema registry is
    // performed)
    try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProperties)) {

      // Subscribe to the topic...
      consumer.subscribe(Collections.singletonList(topic));

      System.out.println("Kafka consumer listening for messages on topic '" + topic + "'");
      // Poll the topic to retrieve records
      while (true) {
        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));

        records.forEach(rec -> System.out.printf("Received a message: offset: %d, partition: %d, key: %s, value: %s\n",
            rec.partition(), rec.offset(), rec.key(), rec.value()));

        // stopping at first non empty record...
        if ( ! records.isEmpty()) {
          break;
        }
      }
    }
  }
}
