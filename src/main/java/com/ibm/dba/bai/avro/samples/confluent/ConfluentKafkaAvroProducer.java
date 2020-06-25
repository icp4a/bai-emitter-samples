/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.BINARY_EMISSION_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.HELP_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_SECURE_PROPERTIES_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_ID_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_VALUE_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.jsonToAvro;
import static com.ibm.dba.bai.avro.samples.KafkaAvroProducerCommon.jsonToAvroBinary;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_LOCATION_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_PASSWORD_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_TYPE_CONFIG;

import com.ibm.dba.bai.avro.samples.BaseSample;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

/**
 * A Kafka Avro producer.
 */
public class ConfluentKafkaAvroProducer {
  private static Properties createKafkaProps(CommandLine cmdLine, boolean binary, Options options) throws Exception {
    Properties props = new Properties();
    // For secure communication with Kafka
    String kafkaSecurityProps = getOption(cmdLine, KAFKA_SECURE_PROPERTIES_ARG);
    try (FileInputStream propsFis = new FileInputStream(new File(kafkaSecurityProps))) {

      props.load(propsFis);
      props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
      if (binary) {
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
      } else {
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
      }

      props.putIfAbsent(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getOption(cmdLine, REGISTRY_URL_ARG));
      props.putIfAbsent(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
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
      usage(options);
      throw ex;
    }
    return props;
  }

  private static void usage(Options options) {
    String header = "Available options :\n";
    String clz = ConfluentKafkaAvroProducer.class.getName();
    String footer = " Example : \n java -cp <path_to>/bai-emitter-samples.jar " + clz + "--" + REGISTRY_URL_ARG
        + "=https://localhost:8084" + "--" + TOPIC_ARG + "=CUSTOMER_V1" + "--" + KAFKA_SECURE_PROPERTIES_ARG
        + "=/path/to/kafka_secure.properties" + "--" + SCHEMA_VALUE_ARG + "=@path/to/schema.avsc" + "--" + EVENT_JSON_ARG
        + "=@path/to/event.json" + "--" + BINARY_EMISSION_ARG;

    BaseSample.usage(clz, options, header, footer);
  }

  /**
   * Main entry point for the Kafka producer.
   * @param args The array of command-line arguments
   * @throws Exception if any error occurs.
   */
  public static void main(String[] args) throws Exception {
    final Options options = buildOptions();
    final CommandLine cmdLine = new DefaultParser().parse(options, args, false);
    if (cmdLine.hasOption(HELP_ARG)) {
      usage(options);
      System.exit(0);
    }
    boolean binary = cmdLine.hasOption(BINARY_EMISSION_ARG);
    final Properties props = createKafkaProps(cmdLine, binary, options);

    final KafkaProducer<String, Object> producer = new KafkaProducer<>(props);

    // Kafka key
    final String key = null;

    // Kafka value
    final Schema.Parser schemaParser = new Schema.Parser().setValidate(false).setValidateDefaults(false);
    final String schemaString = readStringContent(getOption(cmdLine, SCHEMA_VALUE_ARG));
    final Schema schemaValue = schemaParser.parse(schemaString);
    final String event = readStringContent(getOption(cmdLine, EVENT_JSON_ARG));

    Object recordValue = null;
    if (binary) {
      recordValue = jsonToAvroBinary(event, schemaValue);
    } else {
      recordValue = jsonToAvro(event, schemaValue);
    }

    // Metadata
    final String topic = getOption(cmdLine, TOPIC_ARG);
    final Integer partition = null;
    final Iterable<Header> headers = Collections.emptyList();

    ProducerRecord<String, Object> record = new ProducerRecord<>(topic, partition, key, recordValue, headers);
    try {
      producer.send(record);
      System.out.println(
          (binary ? "Binary r" : "R") + "ecord sent: " + (binary ? new String((byte[]) recordValue) : recordValue.toString()));
    } finally {
      producer.flush();
      producer.close();
    }
  }

  private static Options buildOptions() {
    Options rtn = new Options();
    rtn.addRequiredOption(null, REGISTRY_URL_ARG, true, "--" + REGISTRY_URL_ARG + "=<URL of the schema registry>");
    rtn.addRequiredOption(null, TOPIC_ARG, true, "--" + TOPIC_ARG + "=<The Kafka topic to listen to>");
    rtn.addRequiredOption(null, KAFKA_SECURE_PROPERTIES_ARG, true, "--" + KAFKA_SECURE_PROPERTIES_ARG
        + "=<The Java properties file allowing secure communication " + "between the kafka producer and the kafka server>");
    rtn.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
            "--" + KAFKA_USERNAME_ARG + "=<The name of a registry authorized user>");
    rtn.addRequiredOption(null, KAFKA_PASSWORD_ARG, true,
            "--" + KAFKA_PASSWORD_ARG + "=<The password associated to the registry authorized user>");    
    rtn.addRequiredOption(null, SCHEMA_VALUE_ARG, true, "--" + SCHEMA_VALUE_ARG + "=<The Avro schema>");
    rtn.addRequiredOption(null, EVENT_JSON_ARG, true, "--" + EVENT_JSON_ARG + "=<The JSON event>");
    rtn.addOption(null, SCHEMA_ID_ARG, true, "--" + SCHEMA_ID_ARG
        + ": Optional, represents the id assigned to a schema in the registry.");
    rtn.addOption(null, BINARY_EMISSION_ARG, false,
        "--" + BINARY_EMISSION_ARG + "=optional, no value. If present: indicates a preference for binary data exchange"
            + " emission rather than textual data exchange");
    rtn.addOption(null, HELP_ARG, false, "--" + HELP_ARG + ": Optional, prints this help " + "message.");

    return rtn;
  }

  /**
   * Read a string content from the specified source.
   * @param contentSource  if starting with a '@', then it is considered as a file path
   *     and the content is read from this file, otherwise it is considered as the inlined content.
   * @return the content as a string.
   * @throws IOException if any I/O error occurs.
   */
  private static String readStringContent(String contentSource) throws IOException {
    if (contentSource == null) {
      throw new IllegalArgumentException("the event to send was not specified");
    }
    // if starting with a '@', then the content source is a file path
    if (contentSource.startsWith("@")) {
      Path path = Paths.get(contentSource.substring(1));
      if (!Files.exists(path)) {
        throw new IllegalArgumentException("the content source \"" + contentSource + "\" points to a non exisiting file");
      }
      StringBuilder event = new StringBuilder();
      Files.lines(path).forEach(line -> event.append(line).append('\n'));
      return event.toString();
    }
    // otherwise the event source is the inline event
    return contentSource;
  }

  static String getOption(CommandLine cmdLine, String name) throws IllegalArgumentException {
    String value = cmdLine.getOptionValue(name);
    if (value == null) {
      throw new IllegalArgumentException("option " + name + " is missing");
    }
    return value;
  }
}