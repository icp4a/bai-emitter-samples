/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.HELP_ARG;
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
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_LOCATION_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_PASSWORD_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_TYPE_CONFIG;

import com.ibm.dba.bai.avro.samples.BaseSample;
import com.ibm.dba.bai.avro.samples.ManagementServiceClient;
import com.ibm.dba.bai.avro.samples.ProducerCallback;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.avro.Schema;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * This class aims to send an event to a Confluent Avro Kafka server.
 */
public class ConfluentAvro extends BaseSample {

  /**
   * Initialize this sample with the specified command line arguments.
   * @param args the arguments to use.
   */
  public ConfluentAvro(String[] args) {
    super(args);
  }

  @Override
  protected void usage() {
    String header = "Available options :\n";
    String footer = " Example : \n java -cp bai-emitter-samples.jar " + getClass().getName()
        + " --" + SCHEMA_ARG + "/some/file.avsc"
        + " --" + MANAGEMENT_ROOT_URL_ARG + "=https://localhost:8084"
        + " --" + KAFKA_USERNAME_ARG + "=kafka_user"
        + " --" + KAFKA_PASSWORD_ARG + "=kafka_user_password"
        + " --" + KAFKA_SECURE_PROPERTIES_ARG + "=/someDir/kafka-producer-ssl-config.properties"
        + " --" + REGISTRY_URL_ARG + "=https://localhost:9443"
        + " --" + MANAGEMENT_ROOT_URL_ARG + "=https://localhost:8084"
        + " --" + MANAGEMENT_USERNAME_ARG + "=management-Service-User"
        + " --" + MANAGEMENT_PASSWORD_ARG + "=management-Service-User-Password"
        + " --" + EVENT_JSON_ARG + "=/some/file.json";

    usage(getClass().getName(), getOptions(), header, footer);
  }

  @Override
  public Options getOptions() {
    if (this.options == null) {
      this.options = new Options();
      options.addRequiredOption(null, TOPIC_ARG, true,
          "--" + TOPIC_ARG + "=<name of the kafka topic under which the event will be sent>");
      options.addRequiredOption(null, REGISTRY_URL_ARG, true,
          "--" + REGISTRY_URL_ARG + "=<URL of the echema registry>");
      options.addRequiredOption(null, SCHEMA_ARG, true,
          "--" + SCHEMA_ARG + "=<Path to the schema (as a .avsc) file>");
      options.addRequiredOption(null, EVENT_JSON_ARG, true,
          "--" + EVENT_JSON_ARG + "=<Path to the event (as a .json) file to be sent>");
      options.addRequiredOption(null, MANAGEMENT_ROOT_URL_ARG, true,
          "--" + MANAGEMENT_ROOT_URL_ARG + "=<root URL of the management service>");
      options.addRequiredOption(null, MANAGEMENT_USERNAME_ARG, true,
          "--" + MANAGEMENT_USERNAME_ARG + "=<The name of a management service authorized user>");
      options.addRequiredOption(null, MANAGEMENT_PASSWORD_ARG, true,
          "--" + MANAGEMENT_PASSWORD_ARG + "=<The password associated to the management service authorized user>");
      options.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
          "--" + KAFKA_USERNAME_ARG + "=<The name of a registry authorized user>");
      options.addRequiredOption(null, KAFKA_PASSWORD_ARG, true,
          "--" + KAFKA_PASSWORD_ARG + "=<The password associated to the registry authorized user>");
      options.addRequiredOption(null, KAFKA_SECURE_PROPERTIES_ARG, true,
          "--" + KAFKA_SECURE_PROPERTIES_ARG
          + "=<The java properties file allowing secure communication between the kafka producer and the kafka server>");
      options.addOption(null, HELP_ARG, false, "--" + HELP_ARG + ": Optional, prints this help " + "message.");
    }
    return options;
  }

  /**
   * Entry point for testing this class.
   * @param args tyhe command line arguments.
   * @throws Exception if any error occurs.
   */
  public static void main(String[] args) throws Exception {
    final ConfluentAvro confluentAvro = new ConfluentAvro(args);
    if (confluentAvro.initialize()) {
      confluentAvro.sendEvent();
    }
  }

  @Override
  public void sendEvent() throws Exception {
    String rootUrl = getOptionValue(MANAGEMENT_ROOT_URL_ARG);
    String mgntUser = getOptionValue(MANAGEMENT_USERNAME_ARG);
    String mgntPwd = getOptionValue(MANAGEMENT_PASSWORD_ARG);
    String schemaFilePath = getOptionValue(SCHEMA_ARG);
    String topicName = getOptionValue(TOPIC_ARG).trim();
    ManagementServiceClient mngtService = new ManagementServiceClient(rootUrl, mgntUser, mgntPwd);
    if (mngtService.isHealthy()) {
      // ensure the schema is registered
      String schemaStr = readPathContent(schemaFilePath);
      String response = mngtService.sendSchema(schemaStr, topicName,topicName + "-value");
      if (! mngtService.validateSchemaRegistration(response)) {
        throw new RuntimeException("The management service client could not validate schema registration: \n"
            + response);
      } else {
        System.out.println("Schema " + schemaFilePath + " successfully registered");
      }
      // searching the mngtService for the latest version of a schema
      String testEventPath = getOptionValue(EVENT_JSON_ARG);
      Properties kafkaProperties = createKafkaProps();
      // send the event
      ConfluentProducer producer = new ConfluentProducer(kafkaProperties, topicName, testEventPath, schemaFilePath);
      ProducerCallback cb = producer.send();
      if (cb != null) {
        synchronized (cb.getLock()) {
          while (!cb.isResponseReceived()) {
            cb.getLock().wait(500L);
          }
        }
        if (cb.getSentException() == null) {
          System.out.println("Sent event: " + readPathContent(testEventPath));
          new ConfluentConsumer(createConsumerProperties(), topicName, schemaFromJsonString(schemaStr));
        } else {
          System.out.println("Received exception " + cb.getSentException() + " while trying to send the event");
        }
      }
    } else {
      System.out.println("The management service is actually unavailable.");
    }
  }

  private Schema schemaFromJsonString(String schemaStr) {
    Schema.Parser schemaParser = new Schema.Parser();
    Schema schema = schemaParser.parse(schemaStr);
    return schema;
  }

  private  Properties createConsumerProperties() throws Exception {
    Properties props = new Properties();
    // For secure communication with Kafka
    String kafkaSecurityProps = getOptionValue(KAFKA_SECURE_PROPERTIES_ARG);
    try (FileInputStream propsFis = new FileInputStream(new File(kafkaSecurityProps))) {

      props.load(propsFis);

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
      props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getOptionValue(REGISTRY_URL_ARG));
      props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      // considering identical ssl truststore settings for both kafka server and
      // its registry if absent

      if (props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_LOCATION_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
      }
      if (props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_PASSWORD_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
      }
      if (props.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_TYPE_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
      }
    } catch (Exception ex) {
      usage();
      throw ex;
    }

    return props;
  }

  private Properties createKafkaProps() throws Exception {
    Properties props = new Properties();
    // For secure communication with Kafka
    String kafkaSecurityProps = getOptionValue(KAFKA_SECURE_PROPERTIES_ARG);
    try (FileInputStream propsFis = new FileInputStream(new File(kafkaSecurityProps))) {

      props.load(propsFis);
      props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          io.confluent.kafka.serializers.KafkaAvroSerializer.class);

      props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getOptionValue( REGISTRY_URL_ARG));
      props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

      if (props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_LOCATION_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
      }
      if (props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_PASSWORD_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
      }
      if (props.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG) != null) {
        props.putIfAbsent(CONFLUENT_TRUSTORE_TYPE_CONFIG, props.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
      }

    } catch (Exception ex) {
      usage();
      throw ex;
    }
    return props;
  }
}
