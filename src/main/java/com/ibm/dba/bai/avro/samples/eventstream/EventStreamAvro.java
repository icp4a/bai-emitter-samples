/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.eventstream;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_ROOT_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.readPathContent;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_CONSUMER_KEY_DESERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_CONSUMER_VALUE_DESERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_PRODUCER_KEY_SERIALIZER;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.EVENT_STREAM_PRODUCER_VALUE_SERIALIZER;

import com.ibm.dba.bai.avro.samples.BaseSample;
import com.ibm.dba.bai.avro.samples.ManagementServiceClient;
import com.ibm.dba.bai.avro.samples.ProducerCallback;

import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaStrategyAwareSerDe;
import io.apicurio.registry.utils.serde.AvroEncoding;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.FileReader;
import java.util.Base64;
import java.util.Properties;


public class EventStreamAvro extends BaseSample {
  // main args
  public static final String KAFKA_CLIENT_PROPERTIES_FILE_ARG = "kafka-client-props";
  public static final String EVENT_STREAM_PROPERTIES_FILE_ARG = "eventstream-props";
  public static final String EVENT_STREAM_USER_PROPERTY = "event.stream.user";
  public static final String EVENT_STREAM_USER_PASSWORD_PROPERTY = "event.stream.password";

  private final String[] kafkaProperties = {
    SslConfigs.SSL_PROTOCOL_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
  };
  private final String[] apicurioProperties = {
    AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_LOCATION,
    AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_PASSWORD,
    AbstractKafkaStrategyAwareSerDe.REGISTRY_URL_CONFIG_PARAM,
    EVENT_STREAM_USER_PROPERTY,
    EVENT_STREAM_USER_PASSWORD_PROPERTY
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
    String rootUrl = getOptionValue(MANAGEMENT_ROOT_URL_ARG);
    String mgntUser = getOptionValue(MANAGEMENT_USERNAME_ARG);
    String mgntPwd = getOptionValue(MANAGEMENT_PASSWORD_ARG);
    String schemaFilePath = getOptionValue(SCHEMA_ARG);
    String topic = getOptionValue(TOPIC_ARG).trim();
    ManagementServiceClient mngtService = new ManagementServiceClient(rootUrl, mgntUser, mgntPwd);
    if (mngtService.isHealthy()) {
      // ensure the schema is registered
      String response = mngtService.sendSchema(readPathContent(schemaFilePath), topic, topic + "-value");
      if (! mngtService.validateSchemaRegistration(response)) {
        throw new RuntimeException("The management service client could not validate schema registration: \n"
            + response);
      } else {
        System.out.println("Schema " + schemaFilePath + " successfully registered");
      }

      Properties producerProperties = getEventStreamProperties();
      if (checkEventStreamProperties(producerProperties)) {
        String testEvent = readPathContent(getOptionValue(EVENT_JSON_ARG));
        EventStreamProducer producer = new EventStreamProducer(producerProperties, topic, testEvent, schemaFilePath);
        ProducerCallback cb = producer.send();

        if (cb != null) {
          synchronized (cb.getLock()) {
            while (!cb.isResponseReceived()) {
              cb.getLock().wait(500L);
            }
          }
          if (cb.getSentException() == null) {
            System.out.println("Sent event: " + testEvent);
            new EventStreamConsumer(producerProperties, topic);
          } else {
            System.out.println("Received exception " + cb.getSentException() + " while trying to send the event");
          }
        }
      }

    } else {
      System.out.println("Cannot send Event Streams event because not all Event Streams producer properties "
          + "are being satisfied.\n The properties that must be present in kafka properties are:\n"
          + displayOut(kafkaProperties) + "\n and present in apicurio properties are \n"
          + displayOut(apicurioProperties)
          + "\n Important: Note that when kafka and the schema registry use the same truststore,"
          + "\n the values of " + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + " (inside kafka properties file) and "
          + "\n " + AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_LOCATION
          + " (inside EventStream properties file)"
          + "\n must be equal. The same equality rule applies for the values of " + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
          + "\n and " + AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_PASSWORD + ".");
    }
  }

  private String displayOut(String[] props) {
    String rtn = "";
    for (String pname : props ) {
      rtn += "> " + pname + "\n";
    }
    return rtn;
  }

  private Properties getEventStreamProperties() throws Exception {
    Properties properties = new Properties();
    String apicurioConfigFile = getOptionValue(EVENT_STREAM_PROPERTIES_FILE_ARG);
    String kafkaConfigFile = getOptionValue(KAFKA_CLIENT_PROPERTIES_FILE_ARG);
    try (
        FileReader apicurioConfigReader = new FileReader(apicurioConfigFile);
        FileReader kafkaConfigReader = new FileReader(kafkaConfigFile)
    ) {

      properties.load(apicurioConfigReader);
      properties.load(kafkaConfigReader);
      properties.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_ARTIFACT_ID_STRATEGY_CONFIG_PARAM,
              "io.apicurio.registry.utils.serde.strategy.TopicIdStrategy");
      properties.put(EVENT_STREAM_PRODUCER_VALUE_SERIALIZER, "io.apicurio.registry.utils.serde.AvroKafkaSerializer");
      properties.put(EVENT_STREAM_PRODUCER_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
      properties.put(EVENT_STREAM_CONSUMER_VALUE_DESERIALIZER, "io.apicurio.registry.utils.serde.AvroKafkaDeserializer");
      properties.put(EVENT_STREAM_CONSUMER_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");

      // Apicurio registry user authorization header
      String esUser = properties.getProperty(EVENT_STREAM_USER_PROPERTY);
      String esUserPwd = properties.getProperty(EVENT_STREAM_USER_PASSWORD_PROPERTY);
      String encoding = new String(Base64.getEncoder().encode((esUser + ":" + esUserPwd).getBytes()));
      properties.put("apicurio.registry.request.headers.Authorization", "Basic " + encoding);
      // kafka user SCRAM authentication header
      String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
              + "username=\"" + esUser + "\" password=\"" + esUserPwd + "\";";

      properties.putIfAbsent(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

      properties.put(AvroEncoding.AVRO_ENCODING, AvroEncoding.AVRO_BINARY);
      properties.put(AbstractKafkaSerDe.USE_HEADERS, "false");

    }
    return properties;
  }

  private boolean checkEventStreamProperties(Properties producerProperties) {
    for (String propKey : kafkaProperties) {
      if (producerProperties.getProperty(propKey) == null) {
        System.out.println("Warning, mandatory property " + propKey + " is missing");
        return false;
      }
    }
    for (String propKey : apicurioProperties) {
      if (producerProperties.getProperty(propKey) == null) {
        System.out.println("Warning, mandatory property " + propKey + " is missing");
        return false;
      }
    }

    String esUser = producerProperties.getProperty(EVENT_STREAM_USER_PROPERTY);
    String esUserPwd = producerProperties.getProperty(EVENT_STREAM_USER_PASSWORD_PROPERTY);
    if (esUser == null || esUserPwd == null) {
      System.out.println(EVENT_STREAM_USER_PROPERTY + " and " + EVENT_STREAM_USER_PASSWORD_PROPERTY
          + " must be defined in the apicurio properties file");
      return false;
    }

    String securizationProtocol = producerProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if ("SASL_SSL".equals(securizationProtocol)) {
      if (producerProperties.getProperty(SaslConfigs.SASL_MECHANISM) == null
          || producerProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG) == null ) {
        System.out.println("Warning, missing either Property " + SaslConfigs.SASL_MECHANISM + " or "
                + SaslConfigs.SASL_JAAS_CONFIG + " property or both");
        System.out.println("Supported example is:");
        System.out.println("properties.put(SaslConfigs.SASL_MECHANISM, \"SCRAM-SHA-512\"); \n"
                + "String saslJaasConfig = \"org.apache.kafka.common.security.scram.ScramLoginModule required \""
                + "username=\"<username>\" password=\"<password>\";\n"
                + "properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);");
        return false;
      }
    } else {
      // not supported
      System.out.println("The " + securizationProtocol + " securization protocol is not supported.");
      return false;
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
        + " --" + SCHEMA_ARG + "=/some/schema.avsc"
        + " --" + MANAGEMENT_ROOT_URL_ARG + "=https://localhost:8084"
        + " --" + MANAGEMENT_USERNAME_ARG + "management-Service-User"
        + " --" + MANAGEMENT_PASSWORD_ARG + "management-Service-User-Password"
        + " --" + EVENT_JSON_ARG + "=/some/file.json"
        + "\n Important: Note that when kafka and the schema registry use the same truststore,"
        + "\n the values of SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG (inside kafka properties file) and "
        + "\n AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_LOCATION (inside EventStream properties file)"
        + "\n must be equal. The same equality rule applies for the values of SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG"
        + "\n and AbstractKafkaStrategyAwareSerDe.REGISTRY_REQUEST_TRUSTSTORE_PASSWORD";

    BaseSample.usage(getClass().getName(), getOptions(), header, footer);
  }

  @Override
  public Options getOptions() {
    if (this.options == null) {
      this.options = new Options();
      options.addRequiredOption(null, KAFKA_CLIENT_PROPERTIES_FILE_ARG, true,
          "--" + KAFKA_CLIENT_PROPERTIES_FILE_ARG
              + "=<Path to a valid java properties file containing kafka producer configuration properties>");
      options.addRequiredOption(null, EVENT_STREAM_PROPERTIES_FILE_ARG, true,
          "--" + EVENT_STREAM_PROPERTIES_FILE_ARG
              + "=<Path to a valid java properties file containing event stream configuration properties>");
      options.addRequiredOption(null, TOPIC_ARG, true,
          "--" + TOPIC_ARG + "=<name of the Kafka " + "topic the event must be registered against.>");
      options.addRequiredOption(null, EVENT_JSON_ARG, true,
          "--" + EVENT_JSON_ARG + "=<The path to a json file compatible with at least one of the schemas listed");
      options.addRequiredOption(null, SCHEMA_ARG,true,
          "--" + SCHEMA_ARG + "=<The path " + "to a schema (.avsc) file>");
      options.addRequiredOption(null, MANAGEMENT_ROOT_URL_ARG, true,
          "--" + MANAGEMENT_ROOT_URL_ARG + "=<root URL of the management service>");
      options.addRequiredOption(null, MANAGEMENT_USERNAME_ARG, true,
          "--" + MANAGEMENT_USERNAME_ARG + "=<The name of a management service authorized user>");
      options.addRequiredOption(null, MANAGEMENT_PASSWORD_ARG, true,
          "--" + MANAGEMENT_PASSWORD_ARG + "=<The password associated to the management service authorized user>");
    }
    return this.options;
  }

}
