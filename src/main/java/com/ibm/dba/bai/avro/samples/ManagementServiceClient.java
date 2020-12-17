/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
* Client for sending management services requests.
* */
public class ManagementServiceClient {
  private final String rootContextUrl;
  private final String user;
  private final String password;
  private final OkHttpClient httpClient;
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Client constructor.
   * @param rootContext the service root context url.
   * @param usr a properly registered service user name.
   * @param passwd the associated user password.
   * */
  public ManagementServiceClient(String rootContext, String usr, String passwd) {
    this.rootContextUrl = rootContext;
    this.user = usr;
    this.password = passwd;
    this.httpClient = getUnsafeHttpClient(rootContextUrl, user, password);
  }

  /**
   * Indicates management service availability.
   * @return boolean
   * */
  public boolean isHealthy() {
    boolean rtn = false;
    try {
      String healthResponse = sendHealthRequest();
      JsonFactory factory = mapper.getFactory();
      JsonParser jp = factory.createParser(healthResponse);
      JsonNode res  = mapper.readTree(jp);
      int esStatus  = res.findValue("services").get(0).findValue("status").intValue();
      int regStatus = res.findValue("services").get(1).findValue("status").intValue();
      rtn = res.findValue("status").intValue() == 0 && esStatus == 200 && regStatus == 200;
    } catch (IOException exc) {
      return false;
    }
    return rtn;
  }

  /**
   * Sends a json string representation of an Avro schema to the management service for proper registration.
   * @param jsonSchema the json schema.
   * @param indexName the elastic search index used to store the schema
   * @param schemaName the schema name. Must end with "-value", enforced if it is not the case.
   * @return String: The json payload returned by the management service. The registration is successful if
    the returned payload contains a "schemaId" property, otherwise it contains ann error payload.
   * @throws IOException
   * */
  public String sendSchema(String jsonSchema, String indexName, String schemaName) throws IOException {
    // building management service envelope
    ObjectNode baseNode = mapper.createObjectNode()
        .put("index",indexName)
        .put("schemaName", checkSchemaName(schemaName));
    JsonFactory factory = mapper.getFactory();
    JsonParser jp = factory.createParser(jsonSchema);
    JsonNode tree = mapper.readTree(jp);
    // adding schema
    baseNode.set("schema", tree);
    return sendManagementPayload( baseNode.toPrettyString());
  }

  public String sendHealthRequest() throws IOException {
    return sendGetRequest("/api/v1/health?services");
  }

  public boolean validateSchemaRegistration(String response) throws IOException {
    JsonFactory factory = mapper.getFactory();
    JsonNode res = mapper.readTree(factory.createParser(response));
    return res.has("schemaId");
  }

  private String sendManagementPayload(String mngtPayload) throws IOException {
    return sendPostRequest("/api/v1/datasources/elasticsearch/config", mngtPayload);
  }

  private String sendGetRequest(String relativeUrl) throws IOException {
    final Request request = new Request.Builder().url(this.rootContextUrl + relativeUrl).build();
    return httpClient.newCall(request).execute().body().string();
  }

  private String sendPostRequest(String relativeUrl, String jsonManagementPayload) throws IOException {
    final Request req = new Request.Builder().url(this.rootContextUrl + relativeUrl)
        .addHeader("Authorization", Credentials.basic(this.user, this.password))
        .post(RequestBody.create(jsonManagementPayload, MediaType.parse("application/json; charset=utf-8")))
        .build();
    return httpClient.newCall(req).execute().body().string();
  }

  private String checkSchemaName(String schemaName) {
    return schemaName != null && ! schemaName.endsWith("-value") ? schemaName + "-value" : schemaName;
  }

  // utility method for simply handling self signed certificates and basic authenticated requests
  private static OkHttpClient getUnsafeHttpClient(String endpoint, String user, String passwd) {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    OkHttpClient okHttpClient = null;
    try {
      if (endpoint != null && endpoint.trim().startsWith("https")) {
        builder.hostnameVerifier((hostname, session) -> true);

        // Create a trust manager that does not validate certificate chains
        final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
          }

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
          }

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[] {};
          }
        } };

        // Install the all-trusting trust manager
        final SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        // Create an ssl socket factory with our all-trusting manager
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
      }
      if (endpoint != null && user != null && passwd != null) {
        builder.connectTimeout(15, TimeUnit.SECONDS).writeTimeout(15, TimeUnit.SECONDS).readTimeout(15, TimeUnit.SECONDS)
            .addInterceptor(new BasicAuthInterceptor(user, passwd));
      }
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
    okHttpClient = builder.build();
    return okHttpClient;
  }

  /**
   * Utility class used for automatically adding basic authentication headers to authentication requests.
   */
  private static class BasicAuthInterceptor implements Interceptor {
    private final String credentials;

    public BasicAuthInterceptor(String user, String password) {
      this.credentials = Credentials.basic(user, password);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
      Request request = chain.request();
      Request authenticatedRequest = request.newBuilder().header("Authorization", credentials).build();
      return chain.proceed(authenticatedRequest);
    }
  }
}
