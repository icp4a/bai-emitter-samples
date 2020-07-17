/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package confluent;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.BINARY_EMISSION_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_SECURE_PROPERTIES_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.PURGE_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_SUBJECT_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMAS_LIST_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SEPARATOR;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import base.BaseTest;
import com.ibm.dba.bai.avro.samples.confluent.ConfluentAvro;
import com.ibm.dba.bai.avro.samples.confluent.ConfluentAvroRegistry;
import com.ibm.dba.bai.avro.samples.confluent.ConfluentKafkaAvroConsumer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

public class ConfluentRegistryTests extends BaseTest {
  private static boolean serverEnabled = false;
  private static final String Employee_SCHEMA = "{" + " \"schema\": \"" + " {"
      + " \\\"namespace\\\": \\\"com.ibm.employees\\\"," + " \\\"type\\\": \\\"record\\\"," + " \\\"name\\\": \\\"Employee\\\","
      + " \\\"fields\\\": [" + " {\\\"name\\\": \\\"fName\\\", \\\"type\\\": \\\"string\\\"},"
      + " {\\\"name\\\": \\\"lName\\\", \\\"type\\\": \\\"string\\\"},"
      + " {\\\"name\\\": \\\"age\\\",  \\\"type\\\": \\\"int\\\"},"
      + " {\\\"name\\\": \\\"phoneNumber\\\",  \\\"type\\\": \\\"string\\\"}" + " ]" + " }\"" + "}";

  private static final Properties testSchemasResources = new Properties();
  private static final String testSchemasFile =  ConfluentRegistryTests.class.getClassLoader().getResource(
          "confluent/TestSchemas.properties").getFile();
  private static final Properties avroRegistryResources = new Properties();
  private static final Properties testEventsResources = new Properties();

  @BeforeClass
  public static void setupClass() throws Exception {
    testSchemasResources
        .load(ConfluentRegistryTests.class.getClassLoader().getResourceAsStream("confluent/TestSchemas.properties"));
    avroRegistryResources
        .load(ConfluentRegistryTests.class.getClassLoader().getResourceAsStream("confluent/AvroRegistry.properties"));
    testEventsResources
        .load(ConfluentRegistryTests.class.getClassLoader().getResourceAsStream("confluent/TestEvents.properties"));

    BASIC_SCHEMA_V1 = testSchemasResources.getProperty("BASIC_SCHEMA_V1");
    BASIC_SCHEMA_V2 = testSchemasResources.getProperty("BASIC_SCHEMA_V2");
    BASIC_SCHEMA_V3 = testSchemasResources.getProperty("BASIC_SCHEMA_V3");
    testServerEnabled();
  }

  /** Basic sample schema version 1. */
  public static String BASIC_SCHEMA_V1;
  /** Basic sample schema version 2. */
  public static String BASIC_SCHEMA_V2;
  /** Basic sample schema version 3. */
  public static String BASIC_SCHEMA_V3;
  /** Default sample schemas subject for BASIC_SCHEMA_Vxxx schemas. */
  private static final String BASIC_SCHEMA_SUBJECT = "BasicSchemaSubject";

  private static final String[] schemasList = new String[] {
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/CUSTOMER_V1.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/generic/generic-schema.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/generic/event2/generic-schema2.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/generic/event1/generic-schema1.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/basic/employee.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/basic/lessbasic/employee.avsc").getFile(),
      ConfluentRegistryTests.class.getClassLoader().getResource("confluent/basic/muchlessbasic/employee.avsc").getFile()
  };

  // list of subjects associated top schemaList (both lists must have equal length..)
  private static final String[] subjectsList = new String[] {
      "CUSTOMER_V1-value",
      "generic-schema",
      "generic-schema2",
      "generic-schema1",
      "employee",
      "lessbasic-employee",
      "muchlessbasic-employee"
  };

  public static boolean isServerEnabled() {
    return serverEnabled;
  }

  @Test
  public void sendSchemaFromStringTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(
        new String[] { "--" + SCHEMAS_LIST_ARG + "=" + Employee_SCHEMA, "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(),
          "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(), "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(), });
  }

  @Test
  public void sendSchemasFromFilesTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(new String[] { "--" + SCHEMAS_LIST_ARG + "=" + buildSchemasSubjectsList(),
        "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(), "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(),
        "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(), });
  }

  @Test
  public void sendSchemasFromFilesTestwithPurge() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(new String[] { "--" + SCHEMAS_LIST_ARG + "=" + buildSchemasSubjectsList(),
        "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(), "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(),
        "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(), "--" + PURGE_ARG });
  }

  @Test
  public void sendSchemaEmptyListTestWithPurge() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(new String[] { "--" + SCHEMAS_LIST_ARG + "=\"\"", "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(),
        "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(), "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(),
        "--" + PURGE_ARG });
  }

  @Test
  public void sendSchemasNoListTestWithPurge() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(new String[] { "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(),
        "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(), "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(),
        "--" + PURGE_ARG });
  }

  @Test
  public void sendSchemasFromPropertiesTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    getRegistry(new String[] { "--" + SCHEMAS_LIST_ARG + "=" + testSchemasFile,
        "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(), "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(),
        "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(), "--" + PURGE_ARG });
  }

  @Test
  public void sendHelpTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    final String[] args = { "--help" };
    ConfluentAvroRegistry registry = new ConfluentAvroRegistry(args);
    assertFalse(registry.initialize());
  }

  @Test
  public void schemasRestAPITest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    String[] args = new String[] { "--" + SCHEMAS_LIST_ARG + "=" + buildSchemasSubjectsList(),
        "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(), "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(),
        "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(), };
    // initialization and argument schemas registration
    ConfluentAvroRegistry registry = new ConfluentAvroRegistry(args);
    assertTrue(registry.initialize());
    registry.initRegistryState();
    // registering additional schemas
    System.out.println("BASIC_SCHEMA_SUBJECT = " + BASIC_SCHEMA_SUBJECT + ", BASIC_SCHEMA_V1 = " + BASIC_SCHEMA_V1);

    registry.registerNewSchemaVersion(BASIC_SCHEMA_SUBJECT, readConfluentEmbeddedSchemaFile(BASIC_SCHEMA_V1));
    registry.registerNewSchemaVersion(BASIC_SCHEMA_SUBJECT, readConfluentEmbeddedSchemaFile(BASIC_SCHEMA_V2));
    registry.registerNewSchemaVersion(BASIC_SCHEMA_SUBJECT, readConfluentEmbeddedSchemaFile(BASIC_SCHEMA_V3));
    //
    registry.checkRegistered(BASIC_SCHEMA_SUBJECT, readConfluentEmbeddedSchemaFile(BASIC_SCHEMA_V3));
    registry.showFirstVersionOf(BASIC_SCHEMA_SUBJECT);
    registry.showLatestVersionOf(BASIC_SCHEMA_SUBJECT);
    registry.showSchemaById(1);
    registry.showCurrentConfig();
    registry.testCompatibility(BASIC_SCHEMA_SUBJECT, readConfluentEmbeddedSchemaFile(BASIC_SCHEMA_V1));
    registry.setDefaultTopLevelCompatibility(ConfluentAvroRegistry.Compatibility.none);
    registry.setCompatibilityForSubject(BASIC_SCHEMA_SUBJECT, ConfluentAvroRegistry.Compatibility.full);
  }

  private String readConfluentEmbeddedSchemaFile(final String filePath) throws Exception {
    Path schemaPath = Paths.get(filePath);
    StringBuilder content = new StringBuilder();
    content.append("{ \"schema\" : \"");
    Files.lines(schemaPath).forEach(line -> content.append(line.replaceAll("\"", "\\\\\"")));
    content.append("\"}");
    return content.toString();
  }

  private String readNormalSchemaFile(final String filePath) throws Exception {
    Path schemaPath = Paths.get(filePath);
    StringBuilder content = new StringBuilder();
    Files.lines(schemaPath).forEach(line -> content.append(line));
    return content.toString();
  }

  @Test
  public void test_send_event_with_Java_CLI() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    ArrayList<String> args = new ArrayList<>();
    args.add("java");
    args.add("-cp");
    args.add("build/libs/" + AVRO_SAMPLE_JAR_NAME);
    args.add(ConfluentAvro.class.getName());
    launchWithProcessBuilder(buildSendEventArgs(args, false));
  }

  @Test
  public void sendEvent2ConfluentTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    ConfluentAvro.main(buildSendEventArgs(null,false));
  }

  @Test
  public void sendBinaryEvent2ConfluentTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    ConfluentAvro.main(buildSendEventArgs(null,true));
  }

  private String[] buildSendEventArgs(ArrayList<String> initialArgs, boolean binary) {
    ArrayList<String> args = initialArgs == null ? new ArrayList<>() : initialArgs;
    args.add("--" + SCHEMAS_LIST_ARG + "=@" + testSchemasFile);
    args.add("--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint());
    args.add("--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser());
    args.add("--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword());
    args.add("--" + EVENT_JSON_ARG + "=@"
        + ConfluentRegistryTests.class.getClassLoader().getResource("avro-sample-event.json").getFile());
    args.add("--" + REGISTRY_SUBJECT_ARG + "=" + "CUSTOMER_V1-value");
    args.add("--" + KAFKA_SECURE_PROPERTIES_ARG + "="
        + ConfluentRegistryTests.class.getClassLoader().getResource("confluent/KafkaAvroProducer.properties").getFile());
    args.add("--" + PURGE_ARG);
    if (binary) {
      args.add("--" + BINARY_EMISSION_ARG);
    }
    return args.toArray(new String[args.size()]);
  }

  @Test
  public void test_register_schemas_with_Java_CLI() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    String[] args = new String[] { "java", "-cp", "build/libs/" + AVRO_SAMPLE_JAR_NAME, ConfluentAvroRegistry.class.getName(),
        "--" + SCHEMAS_LIST_ARG + "=" + testSchemasFile, "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(),
        "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(), "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(),
        "--" + PURGE_ARG };
    launchWithProcessBuilder(args);
  }

  @Test
  public void testMessagesConsumer() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    ArrayList<String> argsList = new ArrayList<>();
    argsList.add("--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint());
    argsList.add("--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser());
    argsList.add("--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword());
    argsList.add("--" + KAFKA_SECURE_PROPERTIES_ARG + "="
        + ConfluentRegistryTests.class.getClassLoader().getResource("confluent/KafkaAvroProducer.properties").getFile());
    argsList.add("--" + TOPIC_ARG + "=" + "CUSTOMER_V1");
    String[] args = argsList.toArray(new String[argsList.size()]);
    ConfluentKafkaAvroConsumer.main(args);
  }

  private String buildSchemasSubjectsList() {
    String rtn = "";
    for (int id = 0; id < schemasList.length; id++) {
      rtn += subjectsList[id] + "=" + schemasList[id] + SEPARATOR;
    }
    return rtn;
  }

  private static String getRegistryUser() {
    return avroRegistryResources.getProperty(KAFKA_USERNAME_ARG);
  }

  private static String getRegistryPassword() {
    return avroRegistryResources.getProperty(KAFKA_PASSWORD_ARG);
  }

  private static String getRegistryEndpoint() {
    return avroRegistryResources.getProperty(REGISTRY_URL_ARG);
  }

  private static void testServerEnabled() {
    try {
      // purging the registry
      getRegistry(new String[] { "--" + REGISTRY_URL_ARG + "=" + getRegistryEndpoint(),
          "--" + KAFKA_USERNAME_ARG + "=" + getRegistryUser(), "--" + KAFKA_PASSWORD_ARG + "=" + getRegistryPassword(),
          "--" + PURGE_ARG });

      serverEnabled = true;
    } catch (Exception ex) {
      System.out.println("No registry server enabled. Disabling tests");
      serverEnabled = false;
    }
  }

  private static ConfluentAvroRegistry getRegistry(String... args) throws Exception {
    ConfluentAvroRegistry registry = new ConfluentAvroRegistry(args);
    assertTrue(registry.initialize());
    registry.initRegistryState();
    // posting minimal test content from the arguments
    registry.sendSchemas2AvroRegistry();
    registry.showAllVersionsOf(registry.listAllSubjects());
    return registry;
  }
}
