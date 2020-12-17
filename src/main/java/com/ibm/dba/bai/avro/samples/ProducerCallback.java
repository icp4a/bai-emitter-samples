/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {
  private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class);
  private final Object lock = new Object();
  private boolean responseReceived = false;
  private Exception exception = null;
  private final KafkaProducer<String, Object> producer;

  public ProducerCallback(KafkaProducer<String, Object> producer) {
    this.producer = producer;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      this.exception = exception;
      log.error("Exception " + exception.getClass() + ", " + exception.getMessage() + " thrown while sending the event");
    }
    responseReceived = true;
    // Close the producer
    producer.close();
    lock.notify();
  }

  public Object getLock() {
    return lock;
  }

  public Exception getSentException() {
    return exception;
  }

  public boolean isResponseReceived() {
    return responseReceived;
  }
}