/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

import okhttp3.MediaType;

/**
 * Methods common to Kafka Avro producer and consumer.
 */
public class CommandLineConstants {

  public static final String TOPIC_ARG = "topic";
  public static final String KAFKA_SECURE_PROPERTIES_ARG = "kafka-security-properties";
  public static final String KAFKA_USERNAME_ARG = "kafka-username";
  public static final String KAFKA_PASSWORD_ARG = "kafka-password";
  public static final String HELP_ARG = "help";
  public static final String BINARY_EMISSION_ARG = "binary";

  //
  // Confluent Avro constants
  //
  /**
   * The argument name for the URL of the schema registry.
   */
  public static final String REGISTRY_URL_ARG = "registry-url";
  /**
   * Argument for a list of mappings of subject names to corresponding schemas.
   */
  public static final String SCHEMAS_LIST_ARG = "schemas";
  /**
   * Avro media type for Content-type request headers.
   */
  public static final MediaType AVRO_JSON_MEDIA_TYPE = MediaType.parse("application/vnd.schemaregistry.v1+json");
  /**
   * The argument that determines whether to delete all the schemas from the registry.
   */
  public static final String PURGE_ARG = "purge";
  /**
   * Separator character used for building/decoding a list of schema files paths.
   */
  public static final String SEPARATOR = ";";
  /**
   * The subject name argument (Confluent specific).
   */
  public static final String REGISTRY_SUBJECT_ARG = "subject";
  public static final String SCHEMA_VALUE_ARG = "schema-value";
  public static final String SCHEMA_ID_ARG = "schema-id";
  public static final String EVENT_JSON_ARG = "event";
}
