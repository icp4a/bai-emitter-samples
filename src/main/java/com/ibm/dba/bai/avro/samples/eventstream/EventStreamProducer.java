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

import com.ibm.eventstreams.serdes.SchemaInfo;
import com.ibm.eventstreams.serdes.SchemaRegistry;
import com.ibm.eventstreams.serdes.SchemaRegistryConfig;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryApiException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryAuthException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryConnectionException;
import com.ibm.eventstreams.serdes.exceptions.SchemaRegistryServerErrorException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class EventStreamProducer {
  private final Properties props;
  private final String topic;
  private final String event;
  private final String schemaName;
  private final String schemaVersion;

  public EventStreamProducer(Properties props, String topic, String event, String schemaName, String schemaVersion) {
    this.props = props;
    this.topic = topic;
    this.event = event;
    this.schemaName = schemaName;
    this.schemaVersion = schemaVersion;
  }

  /**
   * Sends an event given the arguments passed at construction time.
   * @throws SchemaRegistryApiException possibly sent by the event stream SchemaRegistry
   * @throws SchemaRegistryAuthException possibly sent by the event stream SchemaRegistry
   * @throws NoSuchAlgorithmException possibly sent by the event stream SchemaRegistry
   * @throws KeyManagementException possibly sent by the event stream SchemaRegistry
   * @throws SchemaRegistryServerErrorException possibly sent by the event stream SchemaRegistry
   * @throws SchemaRegistryConnectionException possibly sent by the event stream SchemaRegistry
   * */
  public void send() throws SchemaRegistryApiException, SchemaRegistryAuthException, NoSuchAlgorithmException,
          KeyManagementException, SchemaRegistryServerErrorException, SchemaRegistryConnectionException {

    // Get a new connection to the Schema Registry
    System.out.println("Connecting to the schema registry");
    SchemaRegistry schemaRegistry = new SchemaRegistry(props);
    // Get the schema from the registry
    System.out.printf("Retrieving the schema with name = %s and version = %s\n", schemaName, schemaVersion);
    SchemaInfo schema = schemaRegistry.getSchema(schemaName, schemaVersion);
    final String schemaStr = schema.getSchema().toString();
    System.out.println("Found a schema in the registry: " + schemaStr);

    // Get a new specific KafkaProducer
    try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
      final Schema schemaValue = new Schema.Parser().parse(schemaStr);
      final Object specificRecord = jsonToAvro(event, schemaValue);
      // Prepare the record, adding the Schema Registry headers
      ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, specificRecord);
  
      producerRecord.headers().add(SchemaRegistryConfig.HEADER_MSG_SCHEMA_ID, schema.getIdAsBytes());
      producerRecord.headers().add(SchemaRegistryConfig.HEADER_MSG_SCHEMA_VERSION, schema.getVersionAsBytes());
  
      // Send the record to Kafka
      producer.send(producerRecord);
      System.out.println("Sent event: " + event);
    }
  }
}
