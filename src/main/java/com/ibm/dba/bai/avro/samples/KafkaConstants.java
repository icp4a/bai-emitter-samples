/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

/**
 * Constant for some of the Kafka configurations properties.
 */
public class KafkaConstants {
  //
  // Avro Confluent constants
  //
  /**
   * Location of the schema registry's truststore.
   */
  public static final String CONFLUENT_TRUSTORE_LOCATION_CONFIG = "schema.registry.ssl.truststore.location";
  /**
   * Password for the schema registry's truststore.
   */
  public static final String CONFLUENT_TRUSTORE_PASSWORD_CONFIG = "schema.registry.ssl.truststore.password";
  /**
   * Type of the schema registry's truststore.
   */
  public static final String CONFLUENT_TRUSTORE_TYPE_CONFIG = "schema.registry.ssl.truststore.type";

  //
  // Avro event stream constants
  //

  // key and value de/serializers
  /**
   * Key serializer definition for the Event Streams producer.
   */
  public static final String EVENT_STREAM_PRODUCER_KEY_SERIALIZER = "key.serializer";
  /**
   * Value serializer definition for the Event Streams producer.
   */
  public static final String EVENT_STREAM_PRODUCER_VALUE_SERIALIZER = "value.serializer";
  /**
   * Key deserializer definition for the Event Streams producer.
   */
  public static final String EVENT_STREAM_CONSUMER_KEY_DESERIALIZER = "key.deserializer";
  /**
   * Value deserializer definition for the Event Streams producer.
   */
  public static final String EVENT_STREAM_CONSUMER_VALUE_DESERIALIZER = "value.deserializer";
}
