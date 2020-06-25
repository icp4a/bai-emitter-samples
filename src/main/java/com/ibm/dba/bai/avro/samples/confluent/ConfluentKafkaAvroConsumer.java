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
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.HELP_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_SECURE_PROPERTIES_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_LOCATION_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_PASSWORD_CONFIG;
import static com.ibm.dba.bai.avro.samples.KafkaConstants.CONFLUENT_TRUSTORE_TYPE_CONFIG;
import static com.ibm.dba.bai.avro.samples.confluent.ConfluentKafkaAvroProducer.getOption;

import com.ibm.dba.bai.avro.samples.BaseSample;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Kafka Avro consumer.
 */
public class ConfluentKafkaAvroConsumer {
  /**
   * Maximum number of time no records are polled before exiting the Kafka consumer loop.
   */
  private static final int MAX_NO_MESSAGE_FOUND_COUNT = 1000;
  private static final AtomicInteger numberOfMessagesReceived = new AtomicInteger();
  private static final AtomicBoolean stopped = new AtomicBoolean();

  private static Properties createKafkaProps(CommandLine cmdLine, boolean binary, Options options) throws Exception {
    Properties props = new Properties();
    // For secure communication with Kafka
    String kafkaSecurityProps = getOption(cmdLine, KAFKA_SECURE_PROPERTIES_ARG);
    try (FileInputStream propsFis = new FileInputStream(new File(kafkaSecurityProps))) {

      props.load(propsFis);

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
      if (binary) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
      } else {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
      }
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getOption(cmdLine, REGISTRY_URL_ARG));
      props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
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
    String clz = ConfluentKafkaAvroConsumer.class.getName();
    String footer = " Example : \n java -cp <path_to>/bai-emitter-samples.jar " + clz + "--" + REGISTRY_URL_ARG
        + "=https://localhost:8084" + "--" + TOPIC_ARG + "=CUSTOMER_V1" + "--" + KAFKA_SECURE_PROPERTIES_ARG
        + "=/path/to/kafka_secure.properties" + "--" + KAFKA_USERNAME_ARG + "kafkaUserName" + "--" + KAFKA_PASSWORD_ARG
        + "kafkaUserPassword";

    BaseSample.usage(clz, options, header, footer);
  }

  /**
   * Main entry point for the Kafka consumer loop.
   * @param args The array of command-line arguments.
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

    numberOfMessagesReceived.set(0);
    stopped.set(false);
    int noMessageFound = 0;

    final Properties props = createKafkaProps(cmdLine, binary, options);

    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
      final String topic = getOption(cmdLine, "topic");
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
            break;
          }
        } else {
          noMessageFound = 0;
          numberOfMessagesReceived.addAndGet(consumerRecords.count());

          // print each record.
          consumerRecords
              .forEach(rec -> System.out.printf("Received a message: offset: %d, partition: %d, key: %s, value: %s\n",
                  rec.partition(), rec.offset(), rec.key(), new String(rec.value())));

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

  private static Options buildOptions() {
    Options rtn = new Options();
    rtn.addRequiredOption(null, REGISTRY_URL_ARG, true, "--" + REGISTRY_URL_ARG + "=<URL of the schema registry>");
    rtn.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
        "--" + KAFKA_USERNAME_ARG + "=<The name of a registry authorized user>");
    rtn.addRequiredOption(null, KAFKA_PASSWORD_ARG, true,
        "--" + KAFKA_PASSWORD_ARG + "=<The password associated to the registry authorized user>");
    rtn.addRequiredOption(null, TOPIC_ARG, true, "--" + TOPIC_ARG + "=<The Kafka topic to listen to>");
    rtn.addRequiredOption(null, KAFKA_SECURE_PROPERTIES_ARG, true, "--" + KAFKA_SECURE_PROPERTIES_ARG
        + "=<The Java properties file allowing secure communication " + "between the kafka producer and the kafka server>");
    rtn.addOption(null, BINARY_EMISSION_ARG, false,
        "--" + BINARY_EMISSION_ARG + "=optional, no value. If present: indicates a preference for binary data exchange"
            + " emission rather than textual data exchange");
    rtn.addOption(null, HELP_ARG, false, "--" + HELP_ARG + ": Optional, prints this help " + "message.");
    return rtn;
  }

  public static int getNumberofmessagesreceived() {
    return numberOfMessagesReceived.get();
  }

  public static void stop() {
    stopped.set(true);
  }
}
