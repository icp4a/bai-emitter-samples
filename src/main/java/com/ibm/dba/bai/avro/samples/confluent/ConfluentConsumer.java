/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Kafka Avro consumer.
 */
public class ConfluentConsumer {

  /**
   * Maximum number of time no records are polled before exiting the Kafka consumer loop.
   */
  private static final int MAX_NO_MESSAGE_FOUND_COUNT = 500;
  private static final AtomicInteger numberOfMessagesReceived = new AtomicInteger();
  private static final AtomicBoolean stopped = new AtomicBoolean();

  public ConfluentConsumer(Properties props, String topic, Schema schema) {

    numberOfMessagesReceived.set(0);
    stopped.set(false);
    int noMessageFound = 0;

    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.seekToBeginning(new ArrayList<TopicPartition>());
      System.out.println("Listening for messages on Kafka topic '" + topic + "'");
      consumer.subscribe(Collections.singletonList(topic));

      while (!stopped.get()) {
        if ((getNumberofmessagesreceived() == 0) && (noMessageFound == 0)) {
          System.out.println("Waiting for message ...");
        }

        ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100L));
        if (consumerRecords.count() == 0) {
          noMessageFound++;
          if (noMessageFound > MAX_NO_MESSAGE_FOUND_COUNT) {
            // If the no message found count threshold is reached, exit the
            // consumer loop.
            stop();
          }
        } else {
          noMessageFound = 0;
          numberOfMessagesReceived.addAndGet(consumerRecords.count());

          Iterator it = consumerRecords.iterator();
          // print each record.
          while (it.hasNext()) {
            ConsumerRecord record = (ConsumerRecord) it.next();
            DeserializationHandler desHandler = new DeserializationHandler((byte[])record.value());
            Object message = desHandler.decodeSerializedBuffer(schema, null);
            System.out.printf("Received a message: offset: %d, partition: %d, key: %s, value: %s\n",
                record.partition(),
                record.offset(),
                record.key(),
                message.toString());
          }
          // commits the offset of record to broker.
          consumer.commitAsync();
        }
      }

      if (getNumberofmessagesreceived() == 0) {
        System.err.println("No message found ...");
      } else {
        System.out.println(numberOfMessagesReceived.get() + " message(s) found");
      }
    }
  }

  public static int getNumberofmessagesreceived() {
    return numberOfMessagesReceived.get();
  }

  public static void stop() {
    stopped.set(true);
  }

  class DeserializationHandler {
    private int schemaId;
    private ByteBuffer serializedBuffer;
    private DecoderFactory decoderFactory = DecoderFactory.get();

    /**
     * Creates a deserialization handler to deserialize a payload.
     * @param payload The input payload in the Confluent wire format
     */
    public DeserializationHandler(byte[] payload) {
      parsePayload(payload);
    }

    private void parsePayload(byte[] payload) {
      if (payload == null) {
        throw new RuntimeException("Unacceptable null payload");
      } else if (payload.length < 6) {
        throw new RuntimeException("Payload must be longer than 5 bytes");
      } else if (payload.length > 0 && payload[0] != 0) {
        throw new RuntimeException("Invalid magic byte (must be 0)");
      }
      this.schemaId = ByteBuffer.wrap(Arrays.copyOfRange(payload, 1, 5)).getInt();
      this.serializedBuffer = ByteBuffer.wrap(Arrays.copyOfRange(payload, 5, payload.length));
    }


    Object decodeSerializedBuffer(Schema writerSchema, Schema readerSchema) {
      DatumReader<?> reader = (readerSchema == null) ? new GenericDatumReader<>(writerSchema)
          : new GenericDatumReader<>(writerSchema, readerSchema);
      try {
        Object result = reader.read(null, decoderFactory.binaryDecoder(this.serializedBuffer.array(), null));
        return result;

      } catch (Exception ex) {
        throw new RuntimeException("Deserialization of payload with schemaId " + this.schemaId + " failed", ex);
      }
    }
  }

}
