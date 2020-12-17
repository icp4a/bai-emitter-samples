/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.readPathContent;

import com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon;
import com.ibm.dba.bai.avro.samples.ProducerCallback;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

/**
 * A Kafka Avro producer.
 */
public class ConfluentProducer {

  private final Properties props;
  private final String topic;
  private final String eventPath;
  private final String schemaPath;

  public ConfluentProducer(Properties props, String topic, String event, String schemaPath) {
    this.props = props;
    this.topic = topic;
    this.eventPath = event;
    this.schemaPath = schemaPath;
  }

  /**
   * Sends an event given the arguments passed at construction time.
   * @return EventStreamProducer.EvenStreamCallback to handle the response asynchronously or null
   * @throws Exception any Exception that may occur
   * */
  public ProducerCallback send() throws Exception {

    final KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
    // Kafka key
    final String key = null;
    final Schema.Parser schemaParser = new Schema.Parser().setValidate(false).setValidateDefaults(false);
    final Schema schemaValue = schemaParser.parse(new File(schemaPath));

    final String event = readPathContent(eventPath);

    Object recordValue =  KafkaAvroProducerCommon.jsonToAvro(event, schemaValue);

    // Metadata
    final Integer partition = null;
    final Iterable<Header> headers = Collections.emptyList();

    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, partition, key, recordValue, headers);

    return doSend(producer, producerRecord);
  }

  private ProducerCallback doSend(KafkaProducer<String, Object> producer,
                                        ProducerRecord<String, Object> producerRecord) {
    // Send the record to Kafka
    ProducerCallback callback = new ProducerCallback(producer);
    try {
      producer.send(producerRecord, callback);
    } catch (Exception ex) {
      System.out.println("Got an Exception while waiting for a response : " + ex.getMessage());
      callback.getSentException().fillInStackTrace().printStackTrace();
      producer.close();
      return null;
    }
    return callback;
  }
}