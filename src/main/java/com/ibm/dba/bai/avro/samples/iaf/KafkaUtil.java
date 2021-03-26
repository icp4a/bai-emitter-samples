/**
 * Licensed Materials - Property of IBM
 *  5737-I23
 *  Copyright IBM Corp. 2021. All Rights Reserved.
 *  U.S. Government Users Restricted Rights:
 *  Use, duplication or disclosure restricted by GSA ADP Schedule
 *  Contract with IBM Corp.
 */

package com.ibm.dba.bai.avro.samples.iaf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaUtil {

  public static  void ensureTopicExist(Properties props, String topic) throws Exception {
    AdminClient admin = AdminClient.create(props);
    Set<String> names = admin.listTopics().names().get();
    System.out.println("topics: " + names);
    if (!names.contains(topic)) {
      System.out.printf("topic '%s' does not exist, creating it\n", topic);
      CreateTopicsResult res = admin.createTopics(Stream.of(topic)
          .map(name -> new NewTopic(topic, 3, (short) 1))
          .collect(Collectors.toList())
      );
      final Config config = res.config(topic).get();
      System.out.println("topic " + topic  + " created, config is:" + config.entries());
    } else  {
      System.out.printf("topic '%s' already exist, not creating it\n", topic);
    }

    System.out.printf("configuring topic '%s'\n", topic);
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    // 120s message retention
    final ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "120000");
    System.out.println("adding 120s message retention duration");

    Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<ConfigResource, Collection<AlterConfigOp>>();
    AlterConfigOp op = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);
    updateConfig.put(resource, Collections.singleton(op));
    admin.incrementalAlterConfigs(updateConfig);
  }
}