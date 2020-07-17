/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package eventstreams;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.BINARY_EMISSION_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.EVENT_JSON_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.TOPIC_ARG;
import static com.ibm.dba.bai.avro.samples.eventstream.EventStreamAvro.SCHEMA_NAME_ARG;
import static com.ibm.dba.bai.avro.samples.eventstream.EventStreamAvro.SCHEMA_VERSION_ARG;

import base.BaseTest;
import com.ibm.dba.bai.avro.samples.eventstream.EventStreamAvro;
import com.ibm.eventstreams.serdes.SchemaRegistry;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class EventsStreamRegistryTests extends BaseTest {

  private static boolean serverEnabled = false;
  private static String KAFKA_PROPERTIES;
  private static String EVENTSTREAM_PROPERTIES;
  private static Properties registryProperties;
  private static String EVENT = "@src/test/resources/eventstream/generic-event.json";

  static {
    KAFKA_PROPERTIES = EventsStreamRegistryTests.class.getClassLoader().getResource(
            "eventstream/kafkaProducer.properties").getPath();
    EVENTSTREAM_PROPERTIES = EventsStreamRegistryTests.class.getClassLoader().getResource(
            "eventstream/eventStream.properties").getPath();
    registryProperties = new Properties();
    try (FileReader esr = new FileReader(EVENTSTREAM_PROPERTIES); FileReader kcr = new FileReader(KAFKA_PROPERTIES)) {
      registryProperties.load(esr);
      registryProperties.load(kcr);
      // the EventStream data encoding format (either BINARY or JSON) is decided here thanks to a property
      // and does not need any other code.
      registryProperties.put("com.ibm.eventstreams.schemaregistry.encoding", "BINARY");
    } catch (Exception ex) {
      serverEnabled = false;
    }
  }

  public static boolean isServerEnabled() {
    return serverEnabled;
  }

  @BeforeClass
  public static void testServerEnabled() {
    try {
      new SchemaRegistry(registryProperties);
      serverEnabled = true;
    } catch (Exception ex) {
      serverEnabled = false;
    }
  }

  @Test
  public void test_send_event_with_Java_CLI() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    test_send_event_with_Java_CLI(false);
  }

  public void test_send_event_with_Java_CLI(boolean binaryEmission) throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    launchWithProcessBuilder( buildApplicationArgs( buildTestArgs(), binaryEmission));
  }

  @Test
  public void test_send_binary_event_with_Java_CLI() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    test_send_event_with_Java_CLI(true);
  }

  @Test
  public void help_test_with_Java_CLI() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(buildTestArgs()));
    argsList.add("--help");
    launchWithProcessBuilder(argsList.toArray(new String[argsList.size()]));
  }

  @Test
  public void launchEventStreamJSONTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    EventStreamAvro.main(buildApplicationArgs(null, false));
  }

  @Test
  public void launchEventStreamBinaryTest() throws Exception {
    if (!isServerEnabled()) {
      return;
    }
    EventStreamAvro.main(buildApplicationArgs(null, true));
  }

  private String[] buildTestArgs() {
    return new String[] { "java", "-cp", "build/libs/" + AVRO_SAMPLE_JAR_NAME, EventStreamAvro.class.getName() };
  }

  private String[] buildApplicationArgs(String[] testArgs, boolean binaryEmission) {
    ArrayList<String> args = new ArrayList<>();
    if (testArgs != null) {
      args.addAll(Arrays.asList(testArgs));
    }
    args.add("--" + EventStreamAvro.KAFKA_CLIENT_PROPERTIES_FILE_ARG + "=" + KAFKA_PROPERTIES);
    args.add("--" + EventStreamAvro.EVENT_STREAM_PROPERTIES_FILE_ARG + "=" + EVENTSTREAM_PROPERTIES);
    args.add("--" + EVENT_JSON_ARG + "=" + EVENT);
    args.add("--" + TOPIC_ARG + "=bai-ingress");
    args.add("--" + SCHEMA_NAME_ARG + "=generic");
    args.add("--" + SCHEMA_VERSION_ARG + "=1.0.0");
    if (binaryEmission) {
      args.add("--" + BINARY_EMISSION_ARG);
    }
    return args.toArray(new String[args.size()]);
  }
}
