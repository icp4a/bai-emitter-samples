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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;

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
      DatumReader<Object> reader = new SpecificDatumReader<>(schema);
      DecoderFactory decoderFactory = DecoderFactory.get();
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
   * Serializes a String json event into an avro binary object along a schema for proper registration
   * into a kafka topic.
   * @param jsonEvent the JSON representation of the event to convert. 
   * @param schema the Avro schema to which the event must conform.
   * @return an Avro binary representation of the event.
   */
  public static byte[] jsonToAvroBinary(String jsonEvent, Schema schema) {
    try {
      InputStream in = new ByteArrayInputStream(jsonEvent.getBytes());
      DataInputStream dataIn = new DataInputStream(in);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataIn);
      DatumReader<Object> reader = new GenericDatumReader<>(schema);
      Object datum = reader.read(null, decoder);
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
      writer.write(datum, encoder);
      encoder.flush();
      byte[] encodedMessage = os.toByteArray();
      return encodedMessage;
    } catch (Exception ex) {
      throw new SerializationException(
          String.format("Error serializing json %s to binary Avro of schema %s", jsonEvent, schema), ex);
    }
  }
}
