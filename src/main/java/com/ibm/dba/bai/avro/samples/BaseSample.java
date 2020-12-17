/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.HELP_ARG;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * Base sample for Avro related samples.
 * This class embeds all the necessary api for managing commons options.
 *
 * */
public abstract class BaseSample {
  /**
   * The command line arguments.
   */
  protected final String[] args;
  /**
   * The accepted command-line options.
   */
  protected Options options;

  /**
   * Command line object to handle the options values.
   */
  private CommandLine commandLine;

  /**
   * Initialize this sample with the specified command-line arguments. 
   * @param args the command line arguments.
   */
  public BaseSample(String [] args) {
    this.args = args;
  }

  /**
   * This method provides an initial parsing of the command-line options.
   * @return {@code true} if the initialization is successful, {@code false} otherwise.
   * @throws ParseException if an error occurs while parsing the command line arguments.
   */
  public boolean initialize() throws ParseException {
    if (seekHelp()) {
      // help is sought prior to parsing options otherwise mandatory options
      // presence fails with exception
      usage();
      return false;
    }
    commandLine = new DefaultParser().parse(getOptions(), args, false);
    return true;
  }

  /**
   * Print usage instructions for this sample.
   */
  protected abstract void usage() ;


  public static void usage(String classname, Options options, String header, String footer) {
    org.apache.commons.cli.HelpFormatter formatter = new org.apache.commons.cli.HelpFormatter();
    formatter.setOptionComparator(null);
    String separator = "";
    for (int i = 0; i < formatter.getWidth(); i++) {
      separator += '-';
    }
    separator += '\n';
    String cmdLine = "java -cp bai-event-emitter-samples.jar " + classname + " [options]\n";
    String header2 = separator + header + separator;
    String footer2 = separator + footer;

    formatter.printHelp(cmdLine, header2, options, footer2);
  }

  /**
   * Retrieve the defined options.
   * @return the options for this sample.
   */
  public abstract Options getOptions();

  /**
   * Send an event.
   * @throws Exception if any error occurs.
   */
  public abstract void sendEvent() throws Exception;

  /**
   * Search for --help option presence.
   * @return {@code true} if the "help" argument is specified on the command line, {@code false} otherwise. 
   */
  private boolean seekHelp() {
    for (String arg : args) {
      if ((arg != null) && arg.endsWith(HELP_ARG)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine whetner an option is mandatory.
   * @param name the name of the option.
   * @return {@code true} if the option is mandatory, {@code false} otherwise. 
   */
  protected boolean isOptionMandatory(String name) {
    return getOptions().getRequiredOptions().contains(getOptions().getOption(name));
  }

  /**
   * Returns whether an option is present in the arguments list.
   * @param name the name of the option.
   * @return {@code true} if the option is specified, {@code false} otherwise. 
   */
  protected boolean hasOption(String name) {
    return commandLine != null && commandLine.hasOption(name);
  }

  /**
   * Returns the value of an argument option.
   * @param name the name of the option.
   * @return value of the option if it is specified.
   * @throws IllegalArgumentException if the option is missing.
   */
  public String getOptionValue(String name) throws IllegalArgumentException {
    String value = this.commandLine.getOptionValue(name);
    if (value == null && isOptionMandatory(name)) {
      usage();
      throw new IllegalArgumentException(name);
    }
    return value;
  }
}
