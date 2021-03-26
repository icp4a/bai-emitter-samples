/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package tests;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.dba.bai.avro.samples.ManagementServiceClient;
import com.ibm.dba.bai.avro.samples.confluent.ConfluentAvro;
import com.ibm.dba.bai.avro.samples.iaf.IAFSample;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;


public class SampleTests extends base.BaseTest {

  private static String testEventJson;
  private static String testSchemaJson;
  private static Properties confluentMngtProperties = new Properties();
  private static Properties eventStreamMngtProperties = new Properties();
  private static Properties iafMngtProperties = new Properties();
  private static String topic = "generic-schema";
  private static String kafkaProperties;

  static {
    try {
      confluentMngtProperties.load(new FileInputStream(
          new File(getResource("tests/variables4Confluent.properties"))));
      eventStreamMngtProperties.load(new FileInputStream(
          new File(getResource("tests/variables4EventStream.properties"))));
      testEventJson = readPathContent(getResource(confluentMngtProperties.getProperty("EVENT")));
      testSchemaJson = readPathContent(getResource(confluentMngtProperties.getProperty("SCHEMA")));
      topic = confluentMngtProperties.getProperty("TOPIC");
      kafkaProperties = getResource(confluentMngtProperties.getProperty("KAFKA_SECURITY_PROPERTIES"));
      iafMngtProperties.load(new FileInputStream(
          new File(getResource("tests/variables4IAF.properties"))));
    } catch (Exception ex) {
      ex.fillInStackTrace().printStackTrace();
    }
  }

  private static String getResource(String resourcePath) throws URISyntaxException {
    return SampleTests.class.getClassLoader().getResource(resourcePath).toURI().getPath();
  }

  @Test
  public void testSendSchemaConfluent() throws IOException {
    ManagementServiceClient client = newConfluentManagementClient();
    if (!client.isHealthy()) {
      System.out.println("The Confluent management service is not available. skipping test");
      return;
    }
    String response = client.sendSchema(testSchemaJson,topic,topic);
    JsonNode res = buildResponse(response);
    System.out.println(res.toPrettyString());
  }

  @Test
  public void testSendBadSchemaConfluent() throws Exception {
    ManagementServiceClient client = newConfluentManagementClient();
    if (!client.isHealthy()) {
      System.out.println("The Confluent management service is not available. skipping test");
      return;
    }
    String response = client.sendSchema(readPathContent(getResource("voidSchema.avsc")),"topic4bad","topic4bad");
    JsonNode res = buildResponse(response);
    System.out.println(res.toPrettyString());
  }

  @Test
  public void testSendEvent_Confluent() {
    if (!newConfluentManagementClient().isHealthy()) {
      System.out.println("The Confluent management service is not available. skipping test");
      return;
    }
    try {
      ConfluentAvro.main(buildConfluentArgs());
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testSendEvent_IAF() {
    try {
      ManagementServiceClient service = newIAFManagementClient();
      if (!service.isHealthy()) {
        System.out.println("The IAF management service is not available. skipping test");
        return;
      }
      IAFSample.main(buildIAFArgs());
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testSendEvent_Confluent_WithJavaCli() throws Exception {
    launchWithProcessBuilder(buildConfluentArgs(),ConfluentAvro.class);
  }

  private String[] buildConfluentArgs() throws URISyntaxException {
    ArrayList<String> args = new ArrayList<>();
    args.add("--" + TOPIC_ARG + "=" + topic);
    args.add("--" + SCHEMA_ARG + "=" + getResource(confluentMngtProperties.getProperty("SCHEMA")));
    args.add("--" + EVENT_JSON_ARG + "=" + getResource(confluentMngtProperties.getProperty("EVENT")));
    args.add("--" + MANAGEMENT_ROOT_URL_ARG + "=" + confluentMngtProperties.getProperty("MANAGEMENT_URL"));
    args.add("--" + MANAGEMENT_USERNAME_ARG + "=" + confluentMngtProperties.getProperty("MANAGEMENT_USERNAME"));
    args.add("--" + MANAGEMENT_PASSWORD_ARG + "=" + confluentMngtProperties.getProperty("MANAGEMENT_PASSWORD"));
    args.add("--" + KAFKA_USERNAME_ARG + "=" + confluentMngtProperties.getProperty("KAFKA_USERNAME"));
    args.add("--" + KAFKA_PASSWORD_ARG + "=" + confluentMngtProperties.getProperty("KAFKA_PASSWORD"));
    args.add("--" + REGISTRY_URL_ARG + "=" + confluentMngtProperties.getProperty("REGISTRY_URL"));
    args.add("--" + KAFKA_SECURE_PROPERTIES_ARG + "=" + kafkaProperties);
    return args.toArray(new String[args.size()]);
  }

  private String[] buildIAFArgs() throws URISyntaxException {
    ArrayList<String> args = new ArrayList<>();
    args.add("--" + KAFKA_SECURE_PROPERTIES_ARG
        + "=" + getResource(iafMngtProperties.getProperty("KAFKA_SECURITY_PROPERTIES")));
    args.add("--" + KAFKA_USERNAME_ARG + "=" + iafMngtProperties.getProperty("KAFKA_USERNAME"));
    args.add("--" + KAFKA_PASSWORD_ARG + "=" + iafMngtProperties.getProperty("KAFKA_PASSWORD"));
    args.add("--" + EVENT_JSON_ARG + "=" + getResource(iafMngtProperties.getProperty("EVENT")));
    args.add("--" + SCHEMA_ARG + "=" + getResource(iafMngtProperties.getProperty("SCHEMA")));
    args.add("--" + TOPIC_ARG + "=" + iafMngtProperties.getProperty("TOPIC"));
    args.add("--" + MANAGEMENT_ROOT_URL_ARG + "=" + iafMngtProperties.getProperty("MANAGEMENT_URL"));
    args.add("--" + MANAGEMENT_USERNAME_ARG + "=" + iafMngtProperties.getProperty("MANAGEMENT_USERNAME"));
    args.add("--" + MANAGEMENT_PASSWORD_ARG + "=" + iafMngtProperties.getProperty("MANAGEMENT_PASSWORD"));

    return args.toArray(new String[args.size()]);
  }


  private static ManagementServiceClient newConfluentManagementClient() {
    return newManagementClient( confluentMngtProperties);
  }

  private static ManagementServiceClient newIAFManagementClient() {
    return newManagementClient( iafMngtProperties);
  }

  private static ManagementServiceClient newEventStreamManagementClient() {
    return newManagementClient( eventStreamMngtProperties);
  }

  private static ManagementServiceClient newManagementClient(Properties clientProperties) {
    return new ManagementServiceClient(
        clientProperties.getProperty("MANAGEMENT_URL"),
        clientProperties.getProperty("MANAGEMENT_USERNAME"),
        clientProperties.getProperty("MANAGEMENT_PASSWORD"));
  }

  private static JsonNode buildResponse(String response) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getFactory();
    JsonParser jp = factory.createParser(response);
    return mapper.readTree(jp);
  }
}
