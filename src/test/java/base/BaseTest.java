/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package base;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

public class BaseTest {
  public static final String AVRO_SAMPLE_JAR_NAME = "bai-event-emitter-samples.jar";

  /**
   * Launches a system command based on the provided strings array.
   * @return the process exit code.
   * @throws Exception if any error occurs.
   * */
  public int launchWithProcessBuilder(String[] cmd, Class main) throws Exception {
    String[] cli = Stream.of(buildTestArgs(main), cmd).flatMap(Stream::of).toArray(String[]::new);
    ProcessBuilder builder = new ProcessBuilder(cli);
    builder.redirectErrorStream(true);
    Process process = builder.start();
    InputStream stdout = process.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
    String line = null;
    while ((line = reader.readLine()) != null) {
      System.out.println("Stdout: " + line);
    }
    return process.waitFor();
  }

  private String[] buildTestArgs(Class main) {
    return new String[] { "java", "-cp", "build/libs/" + AVRO_SAMPLE_JAR_NAME, main.getName() };
  }
}
