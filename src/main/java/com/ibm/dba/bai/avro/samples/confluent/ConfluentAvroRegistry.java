/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import static com.ibm.dba.bai.avro.samples.CommandLineConstants.AVRO_JSON_MEDIA_TYPE;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.HELP_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_PASSWORD_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.KAFKA_USERNAME_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.PURGE_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.REGISTRY_URL_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SCHEMAS_LIST_ARG;
import static com.ibm.dba.bai.avro.samples.CommandLineConstants.SEPARATOR;

import com.ibm.dba.bai.avro.samples.BaseSample;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class aims to demonstrate the usage of the Confluent Avro registry REST
 * API. It contains basic schemas (@See BaseSample.BASIC_SCHEMA_V1 ...) as
 * static resources as well as a default subject name (@See
 * BaseSample.BASIC_SCHEMA_SUBJECT) for registering them. It also accepts an
 * '&amp;' separated list of schemas file names as input arguments. The full
 * list of required and optional arguments for the main method of this class is:
 * <ul>
 * <li>`--registry-url=&lt;host:port&gt;`: Mandatory URL of the Avro schema
 * registry</li>
 * <li>`--registry-user=&lt;String&gt;`: Mandatory Avro schema registry user
 * name or api key for authentication</li>
 * <li>`--registry-password=&lt;String&gt;`: Mandatory Avro schema registry user
 * password or api secret for authentication</li>
 * <li>`--schemas=&lt;String&gt;`: Optional `;` character separated list of
 * mapping of subject to paths to avro schema definition files or
 * '=@&lt;String&gt;': location of a java properties files containing a list of
 * 'avro subject = path to .avsc file' lines</li>
 * <li>`--purge`: Optional, no value, allows deleting all existing schemas
 * subjects before running the sample</li>
 * </ul>
 * Note: every schema file provided in the --schemas=&lt;String&gt; arguments
 * list is registered in the avro registry under a subject which name is schema
 * file name without its path. Otherwise, the --schemas=@&lt;String&gt; kind of
 * argument points to a property files as described above. I.E:
 * /some/path/to/someSchema.avsc is registered under the `someSchema.avsc`
 * subject by default
 */
public class ConfluentAvroRegistry extends BaseSample {

  /**
   * Table to associate a schema file path or contents to an Avro subject/topic.
   * The table key is either a schema path or a schema value while the table
   * value is the subject/topic the schema is supposed to be registered under.
   */
  protected Map<String, String> schemaSubjectAssociationTable = new HashMap<>();
  private SchemaRegistryClient client;
  private String avroEndpoint;
  private boolean stateInitialized = false;

  /**
   * Sample constructor.
   * 
   * @param args
   *          the command line arguments.
   */
  public ConfluentAvroRegistry(String[] args) {
    super(args);
  }

  @Override
  public boolean initialize() throws ParseException {
    if (!super.initialize()) {
      return false;
    }
    avroEndpoint = getOptionValue(REGISTRY_URL_ARG);
    String user = getOptionValue(KAFKA_USERNAME_ARG);
    String passwd = getOptionValue(KAFKA_PASSWORD_ARG);
    client = new SchemaRegistryClient(avroEndpoint, user, passwd);
    return true;
  }

  @Override
  public Options getOptions() {
    if (this.options == null) {
      this.options = new Options();
      options.addRequiredOption(null, REGISTRY_URL_ARG, true,
          "--" + REGISTRY_URL_ARG + "=<host:port>: URL of the Avro schema registry.");
      options.addRequiredOption(null, KAFKA_USERNAME_ARG, true,
          "--" + KAFKA_USERNAME_ARG + "=<String>: Mandatory Avro schema registry user name or api key for authentication.");
      options.addRequiredOption(null, KAFKA_PASSWORD_ARG, true, "--" + KAFKA_PASSWORD_ARG
          + "=<String>: Mandatory Avro schema registry user password or api secret for authentication.");
      options.addOption(null, SCHEMAS_LIST_ARG, true,
          "--" + SCHEMAS_LIST_ARG + "=<String>: " + "Optional `" + SEPARATOR
              + "` character separated list of paths to avro schema definition "
              + "files, or \"@\" + path to a java properties file containing <schema_subject=schema_definition> valid "
              + "information.");
      options.addOption(null, PURGE_ARG, false, "--" + PURGE_ARG + ": Optional, no value, if "
          + "present, allows deleting all existing schemas subjects before using the registry.");
      options.addOption(null, HELP_ARG, false, "--" + HELP_ARG + ": Optional, prints this help " + "message.");
    }
    return this.options;
  }

  @Override
  protected void usage() {
    String header = "Available options :\n";

    String footer = " Example : \n java -cp <path_to>/bai-emitter-samples.jar " + getClass().getName() + "    --"
        + REGISTRY_URL_ARG + "=https://localhost:8084\n" + "    --" + KAFKA_USERNAME_ARG + "=avroUser\n" + "    --"
        + KAFKA_PASSWORD_ARG + "=avroUserPassword\n" + "    --" + SCHEMAS_LIST_ARG + " subject1=/path/to/schema1.avsc"
        + SEPARATOR + "subject2=/path/to/schema2.avsc\n" + "    --" + PURGE_ARG + "\n" + "\n"
        + "  Or, Example : \n java -cp <path_to>/bai-emitter-samples.jar " + getClass().getName() + "    --"
        + REGISTRY_URL_ARG + "=https://localhost:8084\n" + "    --" + KAFKA_USERNAME_ARG + "=avroUser\n" + "    --"
        + KAFKA_PASSWORD_ARG + "=avroUserPassword\n" + "    --" + SCHEMAS_LIST_ARG + " @/path/to/schemas_mappings.properties\n"
        + "    --" + PURGE_ARG + "\n";

    usage(getClass().getName(), getOptions(), header, footer);
  }

  @Override
  public void sendEvent() throws Exception {
    // N/A since this is the registry
  }

  /**
   * Initialize the table of mappings of subject ti schemas.
   * 
   * @throws IOException
   *           if any error occurs.
   */
  public void initRegistryState() throws IOException {
    if (!stateInitialized) {
      if (hasOption(SCHEMAS_LIST_ARG)) {
        schemaSubjectAssociationTable = buildSchemasTable(getOptionValue(SCHEMAS_LIST_ARG));
      } else {
        schemaSubjectAssociationTable = new HashMap<>();
      }

      // deleting all previously registered schemas from THIS sample
      purgeRegistry();
      // reset to forward compatibility for schemas as default
      setDefaultTopLevelCompatibility(Compatibility.forward);
      stateInitialized = true;
    }
  }

  /**
   * Purges the registry... if necessary.
   */
  private void purgeRegistry() throws IOException {
    if (hasOption(PURGE_ARG)) {
      deleteAllSchemas();
    }
  }

  /**
   * Sends to an avro registry the list of schemas passed as arguments at
   * initialization time.
   * 
   * @throws IOException
   *           if any error occurs.
   */
  public void sendSchemas2AvroRegistry() throws IOException {
    if (schemaSubjectAssociationTable != null && !schemaSubjectAssociationTable.isEmpty()) {
      System.out.println("Registering schemas ...");
      for (Map.Entry<String, String> entry : schemaSubjectAssociationTable.entrySet()) {
        String subject = entry.getKey();
        String schema = entry.getValue().replaceAll("\n", "");
        registerNewSchemaVersion(subject, schema);
      }
    }
  }

  /**
   * Builds a map containing pairs of subject/schema_path associations, from a
   * "schemas" string argument. This argument has 2 forms: - if it starts with a
   * '@' character, then it is assumed to be the path to a properties file which
   * contains the mappings. - otherwise the argument is assumed to be a ';'
   * (semicolon) separated list of mappings of subjects to schema files. - in
   * the case the argument dose not resolve as a schema file path, it is
   * directly taken as the schema string value. Moreover that, an arbitrary
   * "StringContentSchema" subject is associated to it in the table.
   * 
   * @param schemasPathsList
   *          specifies the subject to schema mappings.
   * @return a map whose keys are subjects and values are the correspondin
   *         schema paths.
   * @throws IOException
   *           if any error occurs.
   */
  protected Map<String, String> buildSchemasTable(String schemasPathsList) throws IOException {
    Properties schemaMappings = new Properties();
    Reader propertiesReader = null;
    try {
      if (schemasPathsList.startsWith("@")) { // the mappings are specified in a properties file
        Path path = Paths.get(schemasPathsList.substring(1));
        if (Files.exists(path)) {
          System.out.println("loading the schema mappings from " + path);
          propertiesReader = Files.newBufferedReader(path);
        } else {
          throw new IllegalArgumentException("the schema mappings argument points to a non-existant file");
        }
      } else { // the mappings are specified in the command-line argument
        System.out.println("loading the schema mappings from the command line");
        String mappings = schemasPathsList.replace(";", "\n");
        propertiesReader = new StringReader(mappings);
      }
      if (propertiesReader == null) {
        throw new IllegalStateException("No schema mapping was found");
      }

      schemaMappings.load(propertiesReader);

    } finally {
      if (propertiesReader != null) {
        propertiesReader.close();
      }
    }

    // for each mapped schema, read its content and put it in the resulting map
    Map<String, String> rtn = new HashMap<>();
    for (Enumeration<?> subjects = schemaMappings.propertyNames(); subjects.hasMoreElements();) {
      String subject = (String) subjects.nextElement();
      Path schemaPath = Paths.get(schemaMappings.getProperty(subject));
      if (!subject.isEmpty() && !schemaPath.toString().isEmpty() && Files.exists(schemaPath)) {
        StringBuilder content = new StringBuilder();
        content.append("{ \"schema\" : \"");
        Files.lines(schemaPath).forEach(line -> content.append(line.replaceAll("\"", "\\\\\"")).append('\n'));
        content.append("\"}");
        rtn.put(subject, content.toString());
      }
    }
    return rtn;
  }

  /**
   * Register a new schema under the specified subject. If successfully
   * registered, this returns the unique identifier of this schema in the
   * registry. The returned identifier should be used to retrieve this schema
   * from the schemas resource and is different from the schema version which is
   * associated with the subject. If the same schema is registered under a
   * different subject, the same identifier will be returned. However, the
   * version of the schema may be different under different subjects.
   *
   * <p>A schema should be compatible with the previously registered schema or
   * schemas (if there are any) as per the configured compatibility level. The
   * configured compatibility level can be obtained by issuing a GET http:get::
   * /config/(string: subject). If that returns null, then GET http:get::
   * /config
   *
   * <p>When there are multiple instances of Schema Registry running in the same
   * cluster, the schema registration request will be forwarded to one of the
   * instances designated as the primary. If the primary is not available, the
   * client will get an error code indicating that the forwarding has failed.
   * 
   * @param schemaSubject the subject name.
   * @param jsonSchema the Avro schema to register.
   * @return the body of the response returned by the schema registry.
   * @throws IOException if any error occurs.
   */
  public String registerNewSchemaVersion(String schemaSubject, String jsonSchema) throws IOException {
    if (avroEndpoint == null) {
      return null;
    }
    schemaSubject = schemaSubject.endsWith("-value") ? schemaSubject : schemaSubject + "-value";
    Request request = new Request.Builder().post(RequestBody.create(jsonSchema, AVRO_JSON_MEDIA_TYPE))
        .url(avroEndpoint + "/subjects/" + schemaSubject + "/versions").build();
    String result = client.getHttpClient().newCall(request).execute().body().string();
    System.out.println("Registered new version of " + schemaSubject + ": ");
    printResult(result);
    return result;
  }

  /**
   * Check if a schema has already been registered under the specified subject.
   * If so, this returns the schema string along with its globally unique
   * identifier, its version under this subject and the subject name
   * 
   * @param schemaSubject the subject name.
   * @param jsonSchema the Avro schema to check.
   * @return the body of the response returned by the schema registry.
   * @throws IOException if any error occurs.
   */
  public String checkRegistered(String schemaSubject, String jsonSchema) throws IOException {
    Request request = new Request.Builder().post(RequestBody.create(jsonSchema, AVRO_JSON_MEDIA_TYPE))
        .url(avroEndpoint + "/subjects/" + schemaSubject).build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Schema to be checked: " + jsonSchema);
    System.out.println("Checking previous registration of a schema with subject " + schemaSubject + ", result is: ");
    printResult(result);
    return result;
  }

  /**
   * Lists all of the subjects existing in this registry.
   * 
   * @return the body of the response returned by the schema registry.
   * @throws IOException if any error occurs.
   */
  @SuppressWarnings("unchecked")
  public List<String> listAllSubjects() throws IOException {
    if (avroEndpoint == null) {
      return new ArrayList<>();
    }
    String result = client.sendRequest(avroEndpoint + "/subjects");
    System.out.println("Registered subjects are: " + result);
    return (ArrayList<String>) parseResultAs(result, ArrayList.class);
  }

  /**
   * Displays all the versions of a given list of schema IDs.
   * 
   * @param schemaIds a list of schema ids.
   * @throws IOException if any error occurs.
   */
  public void showAllVersionsOf(List<String> schemaIds) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    for (String schemaId : schemaIds) {
      String result = client.sendRequest(avroEndpoint + "/subjects/" + schemaId + "/versions/");
      System.out.println("Version(s) of " + schemaId + " is/are: " + result);
    }
  }

  /**
   * Displays the first version of a given subject.
   * @param subject the schema subject name.
   * @throws IOException if any error occurs.
   */
  public void showFirstVersionOf(String subject) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    String result = client.sendRequest(avroEndpoint + "/subjects/" + subject + "/versions/1");
    System.out.println("First version registered under subject " + subject + " is: ");
    printResult(result);
  }

  /**
   * Displays a schema given it's ID.
   * @param id the id of the schema to retrieve.
   * @throws IOException if any error occurs.
   */
  public void showSchemaById(int id) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    String result = client.sendRequest(avroEndpoint + "/schemas/ids/" + id);
    System.out.println("Schema with ID " + id + " is: ");
    printResult(result);
  }

  /**
   * Extracts all the schema registry metadata related to the latest version of
   * a schema registered under a subject.
   * @param subject the sybject that the latest version of the schema is registered under
   * @return a @see #ConfluentAvroRegistry.RegistrySchema instance containing
   *     those metadata, including a schema that can be used to post new events.
   * @throws IOException if any error occurs.
   */
  public RegistrySchema extractSchemaValue(String subject) throws IOException {
    Map<String, Object> map = convertResultAsMap(showLatestVersionOf(subject));
    String schemaSubject = (String) map.get("subject");
    Integer schemaVersion = (Integer) map.get("version");
    Integer schemaID = (Integer) map.get("id");
    String schemaDefinition = (String) map.get("schema");
    final Schema.Parser schemaParser = new Schema.Parser().setValidate(false).setValidateDefaults(false);
    final Schema schemaValue = (schemaDefinition != null ? schemaParser.parse(schemaDefinition) : null);
    // just in case
    String errorMessage = (String) map.get("message");
    Integer errorCode = (Integer) map.get("error_code");
    return new RegistrySchema(schemaID, schemaVersion, schemaSubject, schemaValue, errorCode, errorMessage);
  }

  /**
   * Displays the latest version of a schema registered under a given subject.
   * @param subject the schema subject name.
   * @return the latest version of the schema.
   * @throws IOException if any error occurs.
   */
  public String showLatestVersionOf(String subject) throws IOException {
    if (avroEndpoint == null) {
      return null;
    }
    String result = client.sendRequest(avroEndpoint + "/subjects/" + subject + "/versions/latest");
    System.out.println("Latest version registered under subject " + subject + " is: ");
    printResult(result);
    return result;
  }

  /**
   * Test input schema against the latest version of a subject schema for compatibility.
   * @param schemaSubject the schema subject name.
   * @param jsonSchemaToTest the schema for which to test compatibility.
   * @throws IOException if any error occurs.
   */
  public void testCompatibility(String schemaSubject, String jsonSchemaToTest) throws IOException {
    testCompatibility(schemaSubject, jsonSchemaToTest, "latest");
  }

  /**
   * Test input schema against a particular version of a subject schema for
   * compatibility. Note that the compatibility level applied for the check is
   * the configured compatibility level for the subject (http:get::
   * /config/(string: subject)). If the compatibility level of this subject was
   * never changed, then the global compatibility level applies (http:get:: /config).
   * @param schemaSubject the schema subject name.
   * @param jsonSchemaToTest the schema for which to test compatibility.
   * @param versionID id of the version to test.
   * @throws IOException if any error occurs.
   */
  public void testCompatibility(String schemaSubject, String jsonSchemaToTest, String versionID) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    Request request = new Request.Builder().post(RequestBody.create(jsonSchemaToTest, AVRO_JSON_MEDIA_TYPE))
        .url(avroEndpoint + "/compatibility/subjects/" + schemaSubject + "/versions/" + versionID).build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Compatibility of json schema under subject " + schemaSubject + " is: ");
    printResult(result);
  }

  /**
   * Displays the current compatibility configuration.
   * @throws IOException if any error occurs.
   */
  public void showCurrentConfig() throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    String result = client.sendRequest(avroEndpoint + "/config");
    System.out.println("Current compatibility configuration is: ");
    printResult(result);
  }

  /**
   * Changes the default top level compatibility.
   * @param compatibility the compatibility level to set.
   * @throws IOException if any error occurs.
   */
  public void setDefaultTopLevelCompatibility(Compatibility compatibility) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    Request request = new Request.Builder()
        .put(RequestBody.create("{\"compatibility\": \"" + compatibility + "\"}", AVRO_JSON_MEDIA_TYPE))
        .url(avroEndpoint + "/config").build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Default top level compatibility set to: ");
    printResult(result);
  }

  /**
   * Changes the dafault compatibility for a given subject.
   * @param subject the schema subject name.
   * @param compatibility the compatibility level to set.
   * @throws IOException if any error occurs.
   */
  public void setCompatibilityForSubject(String subject, Compatibility compatibility) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    Request request = new Request.Builder()
        .put(RequestBody.create("{\"compatibility\": \"" + compatibility + "\"}", AVRO_JSON_MEDIA_TYPE))
        .url(avroEndpoint + "/config/" + subject).build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Compatibility for subject " + subject + " set to: ");
    printResult(result);
  }

  /**
   * Purges the registry from any schema.
   * @throws IOException if any error occurs.
   */
  public void deleteAllSchemas() throws IOException {
    System.out.println("Deleting all existing schemas in the registry");
    List<String> subjects = listAllSubjects();
    for (String subject : subjects) {
      deleteAllSchemas(subject);
    }
    listAllSubjects();
  }

  /**
   * Deletes all versions of a given schema registered under a subject.
   * @param subject the schema subject name.
   * @throws IOException if any error occurs.
   */
  public void deleteAllSchemas(String subject) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    Request request = new Request.Builder().delete().url(avroEndpoint + "/subjects/" + subject).build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Deleted all schemas under subject " + subject + " :" + result);
  }

  /**
   * Deletes a specific version of the schema registered under this subject.
   * This only deletes the version and the schema ID remains intact making it
   * still possible to decode data using the schema ID. This API is recommended
   * to be used only in development environments or under extreme circumstances
   * where-in, its required to delete a previously registered schema for
   * compatibility purposes or re-register previously registered schema.
   * 
   * @param subject the schema subject name.
   * @param version the version of the schema to delete.
   * @throws IOException if any error occurs.
   */
  public void deleteSchemaVersion(String subject, String version) throws IOException {
    if (avroEndpoint == null) {
      return;
    }
    Request request = new Request.Builder().delete().url(avroEndpoint + "/subjects/" + subject + "/versions/" + version)
        .build();
    String result = client.newCall(request).execute().body().string();
    System.out.println("Deleted schema version " + version + " under subject " + subject + ": " + result);
  }

  /**
   * Deletes the latest version of a given schema registered under a subject.
   * @param subject the schema subject name.
   * @throws IOException if any error occurs.
   */
  public void deleteLatestSchema(String subject) throws IOException {
    deleteSchemaVersion(subject, "latest");
  }

  /**
   * Retrieve the latest version of a schema for a specified subject.
   * 
   * @param subject
   *          the subject for whcih to retrieve the latest schema version.
   * @return the schema as a json string.
   * @throws IOException
   *           if an error occurs while retrieving the schema.
   */
  public String retieveLatestVersionOf(String subject) throws IOException {
    if (client == null) {
      return null;
    }
    String result = client.sendRequest(avroEndpoint + "/subjects/" + subject + "/versions/latest/schema");
    return result;
  }

  /**
   * Converts JSON result string coming from a REST api response to a Java object.
   */
  private <T> T parseResultAs(String jsonResult, Class<T> type) {
    Object res = Json.parseJson(jsonResult);
    return type.isInstance(res) ? type.cast(res) : null;
  }

  /**
   * Displays a JSON result coming from a REST api response.
   */
  private void printResult(String jsonResults) {
    Map<String, Object> mapResult = convertResultAsMap(jsonResults);
    if (mapResult != null) {
      mapResult.forEach((key, value) -> System.out.println("    " + key + " : " + value));
    } else {
      List<String> listResult = convertResultAsList(jsonResults);
      if (listResult != null) {
        listResult.forEach(value -> System.out.println(value));
      } else {
        System.out.println("non convertible json result " + jsonResults);
      }
    }

  }

  /**
   * Converts a JSON String result to a Map Object.
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> convertResultAsMap(String results) {
    return (Map<String, Object>) parseResultAs(results, HashMap.class);
  }

  /**
   * Converts a JSON String result to a List Object.
   */
  @SuppressWarnings("unchecked")
  private List<String> convertResultAsList(String results) {
    return (List<String>) parseResultAs(results, ArrayList.class);
  }

  /**
   * Possible Avro compatibility enum.
   */
  public enum Compatibility {
    /**
     * The possible levels of compatibility.
     */
    none,
    /**
     * Backward copmpatibility.
     */
    backward,
    /**
     * Backward transitive copmpatibility.
     */
    backward_transitive,
    /**
     * Forward copmpatibility.
     */
    forward,
    /**
     * Forward transitive copmpatibility.
     */
    forward_transitive,
    /**
     * Full copmpatibility.
     */
    full
  }

  /**
   * Utility class to hold a schema and its registration metadata once
   * registered in a Confluent registry.
   */
  public static class RegistrySchema {
    /**
     * The id of the schema in the registry.
     */
    public final int registryId;
    /**
     * The version of the schema in the registry.
     */
    public final int registryVersion;
    /**
     * The topic associated with the schema subject.
     */
    public final String registryTopic;
    /**
     * The actual Avro schema.
     */
    public final Schema registrySchema;
    /**
     * An eventual error code resulting from the schema retrieval request.
     */
    public final int errorCode;
    /**
     * An eventual error message resulting from the schema retrieval request.
     */
    public final String errorMessage;

    /**
     * Build this object.
     * @param id The id of the schema in the registry.
     * @param version  The version of the schema in the registry.
     * @param subject The schema subject.
     * @param schema  The actual Avro schema.
     * @param errorCode An eventual error code resulting from the schema retrieval request.
     * @param errorMessage An eventual error message resulting from the schema retrieval request.
     */
    public RegistrySchema(Integer id, Integer version, String subject, Schema schema, Integer errorCode, String errorMessage) {
      this.registryId = id == null ? -1 : id;
      this.registryVersion = version == null ? -1 : version;
      // the "-value" subject end is specific to Confluent avro and is
      // apparently automatically managed by this platform
      this.registryTopic = subject == null ? ""
          : subject.endsWith("-value") ? subject.substring(0, subject.lastIndexOf("-value")) : subject;

      this.registrySchema = schema;
      this.errorCode = errorCode == null ? -1 : errorCode;
      this.errorMessage = errorMessage == null ? "" : errorMessage;
    }

    /**
     * Determine whether the schema is valid.
     * 
     * @return {@code true} if the schema is valid, {@code false} otherwise.
     */
    public boolean isValidSchema() {
      return errorCode == -1;
    }
  }
}
