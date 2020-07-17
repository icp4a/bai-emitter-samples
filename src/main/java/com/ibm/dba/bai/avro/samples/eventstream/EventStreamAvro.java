/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.eventstream;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.BINARY_EMISSION_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.readStringContent;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_CONSUMER_KEY_DESERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_CONSUMER_VALUE_DESERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_PRODUCER_KEY_SERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_PRODUCER_VALUE_SERIALIZER;

import com.ibm.dba.bai.avro.samples.BaseSample;
import com.ibm.eventstreams.serdes.SchemaRegistryConfig;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.FileReader;
import java.util.Properties;

public class EventStreamAvro extends BaseSample {
  // main args
  public static final String KAFKA_CLIENT_PROPERTIES_FILE_ARG = "kafka-client-props";
  public static final String EVENT_STREAM_PROPERTIES_FILE_ARG = "eventstream-props";
  public static final String SCHEMA_NAME_ARG = "schema-name";
  public static final String SCHEMA_VERSION_ARG = "schema-version";

  private final String[] kafakProperties = {
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SslConfigs.SSL_PROTOCOL_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
      SaslConfigs.SASL_MECHANISM ,
      SaslConfigs.SASL_JAAS_CONFIG
  };
  private final String[] eventStreamsProperties = {
      SchemaRegistryConfig.PROPERTY_API_URL,
      SchemaRegistryConfig.PROPERTY_API_SKIP_SSL_VALIDATION,
      SchemaRegistryConfig.PROPERTY_ENCODING_TYPE,
      EVENT_STREAM_PRODUCER_KEY_SERIALIZER,
      EVENT_STREAM_PRODUCER_VALUE_SERIALIZER,
      EVENT_STREAM_CONSUMER_KEY_DESERIALIZER,
      EVENT_STREAM_CONSUMER_VALUE_DESERIALIZER,
      SchemaRegistryConfig.PROPERTY_BEHAVIOR_TYPE
  };


  public static void main(String[] args) throws Exception {
    final EventStreamAvro eventStreamAvro = new EventStreamAvro(args);
    if (eventStreamAvro.initialize()) {
      eventStreamAvro.sendEvent();
    }
  }

  public EventStreamAvro(String[] args) {
    super(args);
  }

  @Override
  public void sendEvent() throws Exception {
    Properties producerProperties = getProducerProperties(args);
    if (checkProducerProperties(producerProperties)) {
      String topic = getOptionValue(TOPIC_ARG);
      String event = readStringContent(getOptionValue(EVENT_JSON_ARG));
      EventStreamProducer producer = new EventStreamProducer(producerProperties, topic, event,
          getOptionValue(SCHEMA_NAME_ARG), getOptionValue(SCHEMA_VERSION_ARG));
      producer.send();
      new EventStreamConsumer(producerProperties, topic);
    } else {
      System.out.println("Cannot send Event Streams event because not all Event Streams producer properties "
          + "are being satisfied.\n The properties that must be present are:\n" + kafakProperties.toString() + "\n and \n"
          + eventStreamsProperties.toString());
    }
  }

  private Properties getProducerProperties(String[] args) throws Exception {
    Properties properties = new Properties();
    String eventstreamsConfigFile = getOptionValue(EVENT_STREAM_PROPERTIES_FILE_ARG);
    String kafkaConfigFile = getOptionValue(KAFKA_CLIENT_PROPERTIES_FILE_ARG);
    try (FileReader eventstreamsConfigReader = new FileReader(eventstreamsConfigFile);
        FileReader kafkaConfigReader = new FileReader(kafkaConfigFile)) {

      properties.load(eventstreamsConfigReader);
      properties.load(kafkaConfigReader);
      // the EventStream data encoding format (either BINARY or JSON) is decided here thanks to a property
      // and does not need any other code.
      if (hasOption(BINARY_EMISSION_ARG)) {
        properties.put("com.ibm.eventstreams.schemaregistry.encoding", "BINARY");
      } else {
        properties.put("com.ibm.eventstreams.schemaregistry.encoding", "JSON");
      }
    }
    return properties;
  }

  private boolean checkProducerProperties(Properties producerProperties) {
    for (String propKey : kafakProperties) {
      if (producerProperties.getProperty(propKey) == null) {
        System.out.println("Warning, Property " + propKey + " is absent");
        return false;
      }
    }
    for (String propKey : eventStreamsProperties) {
      if (producerProperties.getProperty(propKey) == null) {
        System.out.println("Warning, Property " + propKey + " is absent");
        return false;
      }
    }

    return true;
  }


  @Override
  protected void usage() {
    String header = "Available options :\n";

    String footer = " Example : \n java -cp bai-emitter-samples.jar " + getClass().getName()
        + " --" + KAFKA_CLIENT_PROPERTIES_FILE_ARG + "=some/file.properties "
        + " --" + EVENT_STREAM_PROPERTIES_FILE_ARG + "=some/other/file.properties "
        + " --" + TOPIC_ARG + "=bai-ingress"
        + " --" + SCHEMA_NAME_ARG + "=generic"
        + " --" + SCHEMA_VERSION_ARG + "=1.0.0"
        + " --" + EVENT_JSON_ARG + "=/some/file.json"
        + " --" + BINARY_EMISSION_ARG;

    BaseSample.usage(getClass().getName(), getOptions(), header, footer);
  }

  @Override
  public Options getOptions() {
    if (this.options == null) {
      this.options = new Options();
      options.addRequiredOption(null, KAFKA_CLIENT_PROPERTIES_FILE_ARG, true, "--" + KAFKA_CLIENT_PROPERTIES_FILE_ARG
          + "=<Path to a valid java properties file containing kafka producer " + "configuration properties>");
      options.addRequiredOption(null, EVENT_STREAM_PROPERTIES_FILE_ARG, true, "--" + EVENT_STREAM_PROPERTIES_FILE_ARG
          + "=<Path to a valid java properties file containing event stream " + "configuration properties>");
      options.addRequiredOption(null, TOPIC_ARG, true, "--" + TOPIC_ARG + "=<name of the Kafka "
          + "topic the event must be registered against.>");
      options.addRequiredOption(null, EVENT_JSON_ARG, true,"--" + EVENT_JSON_ARG + "=<The path "
          + "to a json file compatible with at least one of the schemas listed");
      options.addRequiredOption(null, SCHEMA_NAME_ARG,true,"--" + SCHEMA_NAME_ARG + "=<Name "
              + "of the event streams UI previously registered schema>");
      options.addRequiredOption(null, SCHEMA_VERSION_ARG,true,"--" + SCHEMA_VERSION_ARG + "=<version "
              + "of the event streams UI previously registered schema>");
      options.addOption(null, BINARY_EMISSION_ARG, false,
          "--" + BINARY_EMISSION_ARG + "=optional, no value. If present: indicates a preference for binary event"
              + " emission rather than textual event emission");
    }
    return this.options;
  }
}
