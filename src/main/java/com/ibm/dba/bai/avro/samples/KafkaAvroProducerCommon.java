/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Both Confluent and EventStream Avro implementation commonly use org.apache.avro API.
 * This class is where the common code is stored.
 * */
public class KafkaAvroProducerCommon {
  /**
   * Serializes a String json event into an avro human readable object along a schema for proper registration
   * into a kafka topic.
   * @param jsonEvent the JSON representation of the event to convert. 
   * @param schema the Avro schema to which the event must conform.
   * @return an Avro representation of the event.
   */
  public static Object jsonToAvro(String jsonEvent, Schema schema) {
    try {
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      DecoderFactory decoderFactory = new DecoderFactory();
      Decoder decoder = decoderFactory.jsonDecoder(schema, jsonEvent);
      Object object = reader.read(null, decoder);
      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;

    } catch (Exception ex) {
      throw new SerializationException(String.format("Error serializing json %s to Avro of schema %s", jsonEvent, schema), ex);
    }
  }

  /**
   * Read a file content from the specified path source.
   * @param contentSource is considered as a file path.
   * @return the content as a string.
   * @throws IOException if any I/O error occurs.
   */
  public static String readPathContent(String contentSource) throws IOException {
    if (contentSource == null) {
      throw new IllegalArgumentException("the event to send was not specified");
    }
    Path path = Paths.get(contentSource);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("the content source \"" + contentSource + "\" points to a non existing file");
    }
    StringBuilder event = new StringBuilder();
    Files.lines(path).forEach(line -> event.append(line).append('\n'));
    return event.toString();
  }

}
