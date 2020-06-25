/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2020. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.confluent;

import okhttp3.Call;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
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
 * This class abstracts the low-level wiring used to access the schema registry through its REST API.
 */
public class SchemaRegistryClient {
  private final String registryURL;
  private final OkHttpClient httpClient;

  public SchemaRegistryClient(String registryURL, String registryUser, String registryPassword) {
    this.registryURL = registryURL;
    this.httpClient = getUnsafeHttpClient(registryURL, registryUser, registryPassword);
  }

  public String getRegistryURL() {
    return registryURL;
  }

  public OkHttpClient getHttpClient() {
    return httpClient;
  }

  public Call newCall(Request request) {
    return httpClient.newCall(request);
  }

  /**
   * Send a GET request at the specified URL an return the response body as a string.
   * @param url the url for the GET request.
   * @return the body of the response as a string.
   * @throws IOException if any I/O error occurs.
   */
  public String sendRequest(String url) throws IOException {
    final Request request = new Request.Builder().url(url).build();
    return newCall(request).execute().body().string();
  }

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
    private String credentials;

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
