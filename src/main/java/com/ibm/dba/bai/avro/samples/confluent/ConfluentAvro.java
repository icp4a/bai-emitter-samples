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
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.PURGE_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_SUBJECT_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMAS_LIST_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_ID_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMA_VALUE_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;

import com.ibm.dba.bai.avro.samples.BaseSample;
import com.ibm.dba.bai.avro.samples.confluent.ConfluentAvroRegistry.RegistrySchema;
import org.apache.commons.cli.Options;

import java.util.ArrayList;

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
    String footer = " Example : \n java -cp bai-emitter-samples.jar " + getClass().getName() + " --" + SCHEMAS_LIST_ARG
        + "@/some/file.properties" + " --" + REGISTRY_URL_ARG + "=https://localhost:8084" + " --" + KAFKA_USERNAME_ARG
        + "=registry_user" + " --" + KAFKA_PASSWORD_ARG + "=registry_user_password" + " --" + REGISTRY_SUBJECT_ARG
        + "someAvroSubject" + " --" + KAFKA_SECURE_PROPERTIES_ARG + "/someDir/kafka-producer-ssl-config.properties" + " --"
        + EVENT_JSON_ARG + "=/some/file.json";

    usage(getClass().getName(), getOptions(), header, footer);
  }

  @Override
  public Options getOptions() {
    if (this.options == null) {
      this.options = new Options();
      options.addRequiredOption(null, SCHEMAS_LIST_ARG, true,
          "--" + SCHEMAS_LIST_ARG + "=@<Path to a valid java properties file containing a list of"
              + " \"schemas subject name\"=\"schema file path\" values>");
      options.addRequiredOption(null, REGISTRY_URL_ARG, true, "--" + REGISTRY_URL_ARG + "=<URL of the echema registry>");
      options.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
          "--" + KAFKA_USERNAME_ARG + "=<The name of a registry authorized user>");
      options.addRequiredOption(null, KAFKA_PASSWORD_ARG, true,
          "--" + KAFKA_PASSWORD_ARG + "=<The password associated to the registry authorized user>");
      options.addRequiredOption(null, EVENT_JSON_ARG, true,
          "--" + EVENT_JSON_ARG + "=<The path to a json file compatible with at least one of the schemas listed"
              + " using to the \"--" + SCHEMAS_LIST_ARG + "\" argument>");
      options.addRequiredOption(null, REGISTRY_SUBJECT_ARG, true,
          "--" + REGISTRY_SUBJECT_ARG + "=<The avro subject used to send the event under, must be part of the "
              + "subjects listed in the " + SCHEMAS_LIST_ARG + ">");
      options.addRequiredOption(null, KAFKA_SECURE_PROPERTIES_ARG, true, "--" + KAFKA_SECURE_PROPERTIES_ARG
          + "=<The java properties file allowing secure communication " + "between the kafka producer and the kafka server>");
      options.addOption(null, PURGE_ARG, false, "--" + PURGE_ARG
          + ": Optional, no value, if present, allows deleting all existing schemas subjects before using the registry.");
      options.addOption(null, BINARY_EMISSION_ARG, false,
          "--" + BINARY_EMISSION_ARG + "=optional, no value. If present: indicates a preference for binary event"
              + " emission rather than textual event emission");
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
    boolean binaryEmission = isBinaryEmission();
    ConfluentAvroRegistry registry = new ConfluentAvroRegistry(buildRegistryArgs());
    if (registry.initialize()) {
      registry.initRegistryState();
      registry.sendSchemas2AvroRegistry();
      registry.listAllSubjects();
      // searching the registry for the latest version of a schema
      String subject = getOptionValue(REGISTRY_SUBJECT_ARG);
      // for confluent avro registry, the schema can only be retrieved if the
      // subject name ends with "-value"
      subject = subject.endsWith("-value") ? subject : subject + "-value";
      RegistrySchema schemaData = registry.extractSchemaValue(subject);

      if (schemaData.isValidSchema()) {
        String testEvent = getOptionValue(EVENT_JSON_ARG);
        // send the event
        String[] kafkaProducerArgs = buildKafkaProducerArgs(schemaData, registry.getOptionValue(REGISTRY_URL_ARG), testEvent,
            binaryEmission);
        ConfluentKafkaAvroProducer.main(kafkaProducerArgs);
      }
    }
  }

  private String[] buildRegistryArgs() {
    // sending the pre defined customers schemas using the @ compatible
    // argument.
    ArrayList<String> args = new ArrayList<>();

    args.add("--" + SCHEMAS_LIST_ARG + "=" + getSchemasList());
    args.add("--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint());
    args.add("--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser());
    args.add("--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword());
    if (hasOption(PURGE_ARG)) {
      args.add("--" + PURGE_ARG);
    }

    return args.toArray(new String[args.size()]);
  }

  private String[] buildKafkaProducerArgs(ConfluentAvroRegistry.RegistrySchema schemaData, String registryUrl, String testEvent,
      boolean binaryEmission) throws Exception {
    ArrayList<String> args = new ArrayList<>();
    args.add("--" + REGISTRY_URL_ARG + "=" + registryUrl);
    args.add("--" + KAFKA_SECURE_PROPERTIES_ARG + "=" + getOptionValue(KAFKA_SECURE_PROPERTIES_ARG));
    args.add("--" + SCHEMA_VALUE_ARG + "=" + schemaData.registrySchema);
    args.add("--" + TOPIC_ARG + "=" + schemaData.registryTopic);
    args.add("--" + EVENT_JSON_ARG + "=" + testEvent);
    // considering registry and kafka users are the same
    args.add("--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser());
    args.add("--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword());
    if (binaryEmission) {
      args.add("--" + BINARY_EMISSION_ARG);
      args.add("--" + SCHEMA_ID_ARG + "=" + schemaData.registryId);
    }
    return args.toArray(new String[args.size()]);
  }

  private String getSchemasList() {
    String rtn = getOptionValue(SCHEMAS_LIST_ARG);
    return rtn.startsWith("@") ? rtn : "@" + rtn;
  }

  private String getRegistryUser() {
    return getOptionValue(KAFKA_USERNAME_ARG);
  }

  private String getRegistryPassword() {
    return getOptionValue(KAFKA_PASSWORD_ARG);
  }

  private boolean isBinaryEmission() {
    return hasOption(BINARY_EMISSION_ARG);
  }

  private String getRegistryEndpoint() {
    return getOptionValue(REGISTRY_URL_ARG);
  }
}
