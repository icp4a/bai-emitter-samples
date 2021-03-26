/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.iaf;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_SECURE_PROPERTIES_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_ROOT_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.MANAGEMENT_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.readPathContent;

import com.ibm.dba.bai.avro.samples.BaseSample;
import com.ibm.dba.bai.avro.samples.ManagementServiceClient;
import com.ibm.dba.bai.avro.samples.ProducerCallback;

import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaStrategyAwareSerDe;
import io.apicurio.registry.utils.serde.AvroEncoding;
import io.apicurio.registry.utils.serde.AvroKafkaDeserializer;
import io.apicurio.registry.utils.serde.AvroKafkaSerializer;
import io.apicurio.registry.utils.serde.strategy.CachedSchemaIdStrategy;
import io.apicurio.registry.utils.serde.strategy.TopicIdStrategy;
import org.apache.commons.cli.Options;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;

public class IAFSample extends BaseSample {

  /**
   * Initialize this sample with the specified command-line arguments.
   *
   * @param args the command line arguments.
   */
  public IAFSample(String[] args) {
    super(args);
  }

  @Override
  protected void usage() {
    String header = "Available options :\n";

    String footer = " Example : \n java -cp bai-emitter-samples.jar " + getClass().getName()
        + " --" + KAFKA_SECURE_PROPERTIES_ARG + "=some/file.properties "
        + " --" + KAFKA_USERNAME_ARG + "The Kafka user name "
        + " --" + KAFKA_PASSWORD_ARG + "The Kafka user password "
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
      options.addRequiredOption(null, KAFKA_SECURE_PROPERTIES_ARG, true,
          "--" + KAFKA_SECURE_PROPERTIES_ARG
              + "=<Path to a valid java properties file containing kafka producer configuration properties>");
      options.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
          "--" + KAFKA_USERNAME_ARG
              + "=<Path to a valid java properties file containing kafka producer configuration properties>");
      options.addRequiredOption(null, KAFKA_PASSWORD_ARG, true,
          "--" + KAFKA_PASSWORD_ARG
              + "=<Path to a valid java properties file containing kafka producer configuration properties>");
      options.addRequiredOption(null, TOPIC_ARG, true,
          "--" + TOPIC_ARG + "=<name of the Kafka " + "topic the event must be registered against.>");
      options.addRequiredOption(null, EVENT_JSON_ARG, true,
          "--" + EVENT_JSON_ARG + "=<The path to a json file compatible with at least one of the schemas listed");
      options.addRequiredOption(null, SCHEMA_ARG,true,
          "--" + SCHEMA_ARG + "=<The path " + "to a schema (.avsc) file>");
      options.addRequiredOption(null, MANAGEMENT_ROOT_URL_ARG, true,
          "--" + MANAGEMENT_ROOT_URL_ARG + "=<root URL of the management service> (must end with /api)");
      options.addRequiredOption(null, MANAGEMENT_USERNAME_ARG, true,
          "--" + MANAGEMENT_USERNAME_ARG + "=<The name of a management service authorized user>");
      options.addRequiredOption(null, MANAGEMENT_PASSWORD_ARG, true,
          "--" + MANAGEMENT_PASSWORD_ARG + "=<The password associated to the management service authorized user>");
    }
    return this.options;
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

      Properties producerProperties = getIAFProperties();
      // ensuring the topic exists
      KafkaUtil.ensureTopicExist(producerProperties, topic);

      // ensuring the schema is registered
      String response = mngtService.sendSchema(readPathContent(schemaFilePath), topic, topic + "-value");
      if (! mngtService.validateSchemaRegistration(response)) {
        throw new RuntimeException("The management service client could not validate schema registration: \n"
            + response);
      } else {
        System.out.println("Schema " + schemaFilePath + " successfully registered");
      }

      String testEvent = readPathContent(getOptionValue(EVENT_JSON_ARG));
      IAFProducer producer = new IAFProducer(producerProperties, topic, testEvent, schemaFilePath);
      ProducerCallback cb = producer.send();

      if (cb != null) {
        synchronized (cb.getLock()) {
          while (!cb.isResponseReceived()) {
            cb.getLock().wait(500L);
          }
        }
        if (cb.getSentException() == null) {
          System.out.println("Sent event: " + testEvent);
          new IAFConsumer(producerProperties, topic);
        } else {
          System.out.println("Received exception " + cb.getSentException() + " while trying to send the event");
        }
      }

    }
  }

  private Properties getIAFProperties() throws IOException, SecurityException {

    final Properties properties = new Properties();
    String emitterConfig = getOptionValue(KAFKA_SECURE_PROPERTIES_ARG);
    final Path path = Paths.get(emitterConfig).toAbsolutePath();

    try (final BufferedReader reader = Files.newBufferedReader(path)) {
      properties.load(reader);
    }

    checkPropertyExist(properties, AbstractKafkaSerDe.REGISTRY_REQUEST_TRUSTSTORE_LOCATION,null, emitterConfig);
    checkPropertyExist(properties, AbstractKafkaSerDe.REGISTRY_REQUEST_TRUSTSTORE_PASSWORD,null, emitterConfig);
    checkPropertyExist(properties, AbstractKafkaSerDe.REGISTRY_REQUEST_TRUSTSTORE_TYPE,null, emitterConfig);

    // Set the ID strategy to use the fully-qualified schema name (including namespace)
    properties.put(AbstractKafkaSerDe.REGISTRY_CACHED_CONFIG_PARAM, "true");
    properties.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_ARTIFACT_ID_STRATEGY_CONFIG_PARAM,
        TopicIdStrategy.class.getName());
    properties.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_GLOBAL_ID_STRATEGY_CONFIG_PARAM,
        CachedSchemaIdStrategy.class.getName());

    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("key.deserializer", StringDeserializer.class.getName());
    properties.put("value.serializer", AvroKafkaSerializer.class.getName());
    properties.put("value.deserializer", AvroKafkaDeserializer.class.getName());
    properties.put(AbstractKafkaSerDe.USE_HEADERS, "false"); // false to use the wire format Apicurio (8 bytes for globalId)
    properties.put(AvroEncoding.AVRO_ENCODING, AvroEncoding.AVRO_BINARY);

    properties.put("ssl.protocol", "TLSv1.2");
    properties.put("ssl.enabled.protocol", "TLSv1.2");
    properties.put("ssl.endpoint.identification.algorithm", "");
    //
    checkPropertyExist(properties, "ssl.truststore.location",null, emitterConfig);
    checkPropertyExist(properties, "ssl.truststore.password", null, emitterConfig);
    checkPropertyExist(properties, "ssl.truststore.type", "pkcs12", emitterConfig);

    checkPropertyExist(properties, "bootstrap.servers", null, emitterConfig);

    checkPropertyExist(properties, "security.protocol",  "SASL_SSL", emitterConfig);
    checkPropertyExist(properties, "sasl.mechanism", "SCRAM-SHA-512", emitterConfig);
    checkPropertyExist(properties, "apicurio.registry.url", null, emitterConfig);
    // checking the registry url ends with /api
    if ( ! properties.getProperty("apicurio.registry.url").endsWith("/api")) {
      System.out.println("The apicurio.registry.url property must end with \"/api\", exiting.");
      System.exit(1);
    }
    checkPropertyExist(properties, AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, getOptionValue(REGISTRY_URL_ARG),
        emitterConfig);
    checkPropertyExist(properties, "sasl.jaas.config", buildJaasConfig(), emitterConfig);
    String kafkaUser = getOptionValue(KAFKA_USERNAME_ARG);
    String kafkaPwd = getOptionValue(KAFKA_PASSWORD_ARG);

    final String auth = kafkaUser + ":" + kafkaPwd;
    final String encodedAuth = new String(Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8)),
        StandardCharsets.UTF_8);
    properties.put("apicurio.registry.request.headers.Authorization", "Basic " + encodedAuth);
    properties.put(AvroEncoding.AVRO_ENCODING, AvroEncoding.AVRO_BINARY);

    return properties;
  }

  private String buildJaasConfig() {
    // if the jaas config is not set in the properties, we obtain the kafka user and pssaword from environment variables
    String kafkaUser = getOptionValue(KAFKA_USERNAME_ARG);
    String kafkaPwd = getOptionValue(KAFKA_PASSWORD_ARG);
    String jaasConfig = String.format(
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
        kafkaUser,
        kafkaPwd);
    return jaasConfig;
  }

  private static String checkPropertyExist(Properties properties, String propertyName, String defaultValue, String source) {
    String propertyValue = properties.getProperty(propertyName);
    if (propertyValue == null && defaultValue == null) {
      System.out.println("The property \"" + propertyName + "\" value must be present in " + source);
      System.exit(1);
    } else if (propertyValue == null && defaultValue != null) {

      System.out.println("Using default value of \""
          + (defaultValue.contains("password") ? "*********" : defaultValue)
          + "\" for property name \""
          + propertyName + "\"");
      propertyValue = defaultValue;
      properties.setProperty(propertyName, defaultValue);
    }
    return propertyValue;
  }

  public static void main(String[] args) throws Exception {
    final IAFSample iafSample = new IAFSample(args);
    if (iafSample.initialize()) {
      iafSample.sendEvent();
    }
  }

}
