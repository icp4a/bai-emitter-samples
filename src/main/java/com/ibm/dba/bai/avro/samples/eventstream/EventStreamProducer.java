/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.eventstream;

import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.jsonToAvro;

import com.ibm.dba.bai.avro.samples.ProducerCallback;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import javax.ws.rs.WebApplicationException;

public class EventStreamProducer {

  private final Properties props;
  private final String topic;
  private final String event;
  private final String schemaPath;

  public EventStreamProducer(Properties props, String topic, String event, String schemaPath) {
    this.props = props;
    this.topic = topic;
    this.event = event;
    this.schemaPath = schemaPath;
  }

  /**
   * Sends an event given the arguments passed at construction time.
   * @return ProducerCallback to handle the response asynchronously or null
   * @throws Exception any Exception that may occur
   * */
  public ProducerCallback send() throws Exception {

    Schema.Parser schemaDefinitionParser = new Schema.Parser();//.setValidate(false).setValidateDefaults(false);
    Schema schema = schemaDefinitionParser.parse(new File(schemaPath));

    // the string event is supposed to be conformal to the schema
    Object eventObject = jsonToAvro(event, schema);

    KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, eventObject);
    return doSend(producer, producerRecord);
  }
  
  private ProducerCallback doSend(KafkaProducer<String, Object> producer,
                                          ProducerRecord<String, Object> producerRecord) {
    ProducerCallback callback = new ProducerCallback(producer);
    try {
      producer.send(producerRecord, callback);
    } catch (WebApplicationException exc) {
      System.out.println("Got a WebApplicationException as response : " + exc.getResponse().getStatus()
              + " : " + exc.getMessage());
      if (exc.getResponse().getStatus() == 404) {
        System.out.println("Maybe the schema was not present in the schema registry");
      }
      producer.close();
      return null;
    } catch (Exception ex) {
      System.out.println("Got an Exception while waiting for a response : " + ex.getMessage());
      callback.getSentException().fillInStackTrace().printStackTrace();
      producer.close();
      return null;
    }
    return callback;
  }
}
