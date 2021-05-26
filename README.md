# Samples for IBM Business Automation Insights custom event emitter

This directory provides samples related to using custom events with Business Automation Insights.
The samples demonstrate how to:

- Register an Avro schema.
- Send to Business Automation Insights events that conform to the registered schema, by using either Confluent or
  IBM Automation Foundation Avro APIs.

Before sending events, you must configure Business Automation Insights to process custom events.
For more information about such configuration, see the
[IBM Knowledge Center](https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/con_bai_custom_events.html).

## Table of contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- Table of Contents generated with [DocToc](https://github.com/thlorenz/doctoc) -->

- [How to build the samples](#how-to-build-the-samples)
- [Prerequisites](#prerequisites)
- [Confluent](#confluent)
  - [Schema example](#schema-example)
  - [Schema registration](#schema-registration)
  - [Event example](#event-example)
  - [Running the sample](#running-the-sample)
    - [Example output](#example-output)
  - [Java code to send an event](#java-code-to-send-an-event)
    - [Retrieve a schema](#retrieve-a-schema)
    - [Java code to register a schema](#java-code-to-register-a-schema)
    - [Java code to convert the event JSON payload to binary representation by using a schema](#java-code-to-convert-the-event-json-payload-to-binary-representation-by-using-a-schema)
    - [Java code to send an event as a binary payload](#java-code-to-send-an-event-as-a-binary-payload)
- [IBM Automation Foundation](#ibm-automation-foundation)
  - [IBM Automation Foundation Avro schema example](#ibm-automation-foundation-avro-schema-example)
  - [Corresponding IBM Automation Foundation event example](#corresponding-ibm-automation-foundation-event-example)
  - [Running the IBM Automation Foundation sample](#running-the-ibm-automation-foundation-sample)
    - [Example output with IBM Automation Foundation](#example-output-with-ibm-automation-foundation)
  - [Java code to send an event with IBM Automation Foundation](#java-code-to-send-an-event-with-ibm-automation-foundation)
    - [Procedure to register a schema with IBM Automation Foundation](#procedure-to-register-a-schema-with-ibm-automation-foundation)
    - [Convert the event JSON payload to binary representation by using a schema with IBM Automation Foundation](#convert-the-event-json-payload-to-binary-representation-by-using-a-schema-with-ibm-automation-foundation)
    - [Send an event as a binary payload with the IBM Automation Foundation API](#send-an-event-as-a-binary-payload-with-the-ibm-automation-foundation-api)
- [Tests](#tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## How to build the samples

You build the samples with the `./gradlew clean jar` command. This command produces the
 `./build/libs/bai-event-emitter-samples.jar` JAR file.

The `build/libs/bai-event-emitter-samples.jar` file is a fat JAR which contains several Java main entry points.
These entry points are used by the launch scripts.

## Prerequisites

You need:

- IBM SDK, Java Technology Edition, Version 8
- a .properties file for Kafka security configuration, such as
  [KafkaAvroProducer.properties](./src/test/resources/confluent/KafkaAvroProducer.properties)
- A Kafka installation: either  
  - Confluent Kafka 5.4.1 coming with IBM Business Automation Insight for a server
  - IBM Automation Foundation (IAF) Kafka coming with IBM Business Automation Insights for Kubernetes
- an Avro schema as an `.avsc` file (see the [schema example](#schema-example) below)
- an event to send, as a file in JSON format (`.json` file) corresponding to the Avro schema

One benefit of using the docker-compose packaging of IBM Business Automation Insights is that this deployment mode is
 lightweight because of its single server architecture. One additional benefit is that it comes with an embedded
 Confluent Kafka distribution, which you can either use as is or replace with your own. Also, be aware that if you
 scale a single server deployment, you must restart the platform.
On the other hand, the Kubernetes deployment mode is more complex to install but offers the inherent
scalability and high availability of the platform.

## Confluent

When you use IBM Business Automation Insights in single server deployment mode,
you can run the Confluent Kafka and Schema Registry containers.
This is how you run this sample with Confluent.

For Business Automation Insights to support your custom events, you need to configure how the events are routed from
Kafka to Elasticsearch and, optionally, to HDFS, by modifying the `config/flink/event-processor-config.yml`
configuration file, as explained
[here](https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_proc.html)

Here is an example of the `event-processor-config.yml` file:

```yaml
##
## Configurations file to define flows of events in the Flink Event Processor Job.
##
## Syntax:
##  The overall configuration is defined as an array of configurations:
##   configuration:
##     [{configuration1}, {configuration2}, ...]
##
##  Each configuration contains:
##    A single Kafka topic
##    A single Elasticsearch index bound to the topic
##    An optional HDFS bucket bound to the topic
##
## Example:
##  configurations:
##    - kafka-topic: <topic name 1>
##      elasticsearch-index: <Elasticsearch index name 1>
##      hdfs-bucket: <hdfs://hdfs-hostname/bucket-name 1>
##    - kafka-topic: <topic name 2>
##      elasticsearch-index: <Elasticsearch index name 2>
---
configurations:
    - kafka-topic: generic-schema-topic
      elasticsearch-index: generic-schema-index
    - kafka-topic: another-schema-ToPiC
      elasticsearch-index: another-schema-idx
```

In this example, Business Automation Insights forwards events from the `generic-schema-topic` Kafka topic to the
 `generic-schema-index` Elasticsearch index.  
This sample considers that it is a best practice to use the same value for
 both Kafka topic and Elasticsearch parameters.

### Schema example

A schema is a structure in JSON format which is generally located in a `.avsc` file. Here is an example:

```json
{
  "name": "generic",
  "type": "record",
  "namespace": "com.ibm.bai",
  "fields": [
    {
      "name": "timestamp",
      "logicalType": "timestamp-millis",
      "type": "long"
    },
    {
      "name": "order",
      "type": "string"
    },
    {
      "name": "total_price",
      "type": "int"
    },
    {
      "name": "products",
      "type": {
        "type": "array",
        "items": {
          "name": "products_record",
          "type": "record",
          "fields": [
            {
              "name": "product_id",
              "type": "string",
              "ibm.automation.identifier": true
            },
            {
              "name": "description",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "quantity",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
```

NOTE: the `"ibm.automation.identifier": true` field attribute is optional and specific to IBM Business Automation
 Insights.  
This attribute allows the user to create an identifier that can be used later for creating some monitoring sources.  
For more information, see the following documentation pages:

- [Business Automation Insights for server related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_config_monitsource.html)
- [Business Automation Insights for Kubernetes related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/tsk_bai_k8s_cust_event_config_monitsource.html)

You can also find this schema [here](src/test/resources/confluent/generic/generic-schema.avsc).

### Schema registration

The Avro schema must be registered in an Avro registry. The schema validates and encodes events.
You do not need to directly access the schema registry to register the schema because such registration is the purpose  
of the management service.  
For more information about the schema specification, see the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).

### Event example

The following event complies with the Avro schema that is presented in the previous section:

```json
{
  "timestamp": 27,
  "order": "3d478-36e",
  "total_price": 500,
  "products": [
    {
      "product_id": "First product",
      "description": null,
      "quantity": 1
    },
    {
      "product_id": "Second product",
      "description": {
      "string": "Fragile"  
      },
      "quantity": 2
    }
  ]
}
```

You can find the corresponding file [here](src/test/resources/confluent/generic/event-4-generic.json).

### Running the sample

1. Start Business Automation Insights for a server.
2. Edit the [confluent.config](confluent.config) file.
    - Set the topic name: `TOPIC=generic-schema`
    - Set the Kafka credentials: `KAFKA_USERNAME=<kafka_user>` and `KAFKA_PASSWORD=<kafka_password>`
    - Set the management service credentials: `MANAGEMENT_USERNAME=<mngt_user>` and `MANAGEMENT_PASSWORD=<mngt_password>`
    - Set the management service URL: `MANAGEMENT_URL=https://localhost:6898`
    - Set the Kafka registry URL: `REGISTRY_URL=https://localhost:8084`
    - Set the event source file: `EVENT=src/test/resources/avro-sample-event.json`
    - Set the schema source file: `src/test/resources/confluent/generic/generic-schema.avsc`
    - Set the path to a Kafka security properties file:
    `KAFKA_SECURITY_PROPERTIES=src/test/resources/confluent/KafkaAvroProducer.properties`
3. Edit the Kafka security properties file, example:
  [KafkaAvroProducer.properties](./src/test/resources/confluent/KafkaAvroProducer.properties)
4. Compile the sample: `./gradlew clean jar`
5. Run the sample: `bin/run-confluent-sample`. The output is similar to
  [this example output](#example-output)
6. To verify that the event sent with the sample is processed by Business Automation Insights, check that the
  Elasticsearch index `generic-schema` is created, for example:
  `curl -X GET -u <admin_user>:<admin_password> -k https://localhost:"$ELASTICSEARCH_PORT"/_cat/indices/generic-schema?h=index,
  docs.count\&v`

#### Example output

```text
KAFKA_SECURITY_PROPERTIES is src/test/resources/confluent/KafkaAvroProducer.properties
MANAGEMENT_URL is https://localhost:6898
SCHEMA is src/test/resources/confluent/generic/generic-schema.avsc
KAFKA_USERNAME is admin
KAFKA_PASSWORD is **********
MANAGEMENT_USERNAME is admin
MANAGEMENT_PASSWORD is **********
TOPIC is generic-schema
EVENT is src/test/resources/confluent/generic/event-4-generic.json
REGISTRY_URL is https://localhost:8084
Schema src/test/resources/confluent/generic/generic-schema.avsc successfully registered
Sent event: {
  "timestamp": 27,
  "order": "3d478-36e",
  "total_price": 500,
  "products": [
    {
      "product_id": "First product",
      "description": null,
      "quantity": 1
    },
    {
      "product_id": "Second product",
      "description": {
        "string": "Fragile"  
      },
      "quantity": 2
    }
  ]
}

Listening for messages on Kafka topic 'generic-schema'
Waiting for message ...
Received a message: offset: 0, partition: 26, key: null, value: {"timestamp": 27,
"order": "3d478-36e", "total_price": 500,
"products": [{"product_id": "First product", "description": null, "quantity": 1}, {"product_id": "Second product",
"description": "Fragile", "quantity": 2}]}
1 message(s) found

```

### Java code to send an event

Sending an event requires four steps: retrieve a schema, ensure the schema registration, convert the event to binary
and send this binary payload.

#### Retrieve a schema

You can retrieve a schema either from a schema registry or by reading the schema from a `.avsc` file.
Retrieving a schema from the schema registry is not a feature that is used in the present samples because the schema
 definition is already known.  

#### Java code to register a schema

You upload a schema to the Confluent Avro registry through the management service.  
 The [management service](./src/main/java/com/ibm/dba/bai/avro/samples/ManagementServiceClient.java)
 needs an Elasticsearch index name, a schema name, and the schema itself as a JSON string.  
 This schema name defines both the subject used to retrieve a schema from the registry **and** the Kafka topic used to
 send an event.  
 Because the topic name is stored under an Elasticsearch index, a good naming convention is to use the same
 value for both the topic name and Elasticsearch index name.  
 The relationship between the subject and the topic is described here:
 [What is a topic versus a schema versus a subject?](https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html#terminology-review)

This sample uses the
[Confluent default name strategy](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#sr-schemas-subject-name-strategy)
where the topic name is the name that was used at schema registration time. The subject name is deduced from the topic
name by adding a `-value` keyword to the topic name.

The code of the sample uses the management service REST API in the `sendSchema` method of the
 [ManagementServiceClient class](./src/main/java/com/ibm/dba/bai/avro/samples/ManagementServiceClient.java).

```java
    // Content of the src/test/resources/confluent/generic/generic-schema.avsc file
    String jsonSchema = ...;
    String managementRootUrl = ...;
    String managementUser = ...;
    String managementPwd = ...;
    String topicName = ...;
    ManagementServiceClient mngtService = new ManagementServiceClient(managementRootUrl, managementUser, managementPwd);
    if (mngtService.initialize()) {
      // ensure the schema is registered
      String response = mngtService.sendSchema(jsonSchema, topicName, topicName + "-value");
      if(mngtService.validateSchemaRegistration(response)) ...
    }
```

#### Java code to convert the event JSON payload to binary representation by using a schema

```java
Schema schema = ...;
String jsonEvent = ...;

GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema)
DecoderFactory decoderFactory = new DecoderFactory();
Decoder decoder = decoderFactory.jsonDecoder(schema, jsonEvent);
Object encodedMessage = reader.read(null, decoder);
if (schema.getType().equals(Schema.Type.STRING)) {
  encodedMessage = ((Utf8) encodedMessage).toString();
}
return encodedMessage;

```

You can find this code in
[KafkaAvroProducerCommon](./src/main/java/com/ibm/dba/bai/avro/samples/KafkaAvroProducerCommon.java)
in the sample code.

#### Java code to send an event as a binary payload

```java
// Read the Kafka security properties and create a producer (see the "Prerequisites" section)
Properties kafkaProps = ...;
final KafkaProducer<String, Object> producer = new KafkaProducer<>(kafkaProps);

Object binaryEvent = ...;
String topic = ...;

ProducerRecord<String, Object> record =
    new ProducerRecord<>(topic, null, null, binaryEvent, Collections.emptyList());
producer.send(record);
```

You can find this code in [ConfluentProducer](./src/main/java/com/ibm/dba/bai/avro/samples/confluent/ConfluentProducer.java).

## IBM Automation Foundation

IBM Business Automation Insights Kubernetes deployment mode allows you to run the IBM Automation Foundation Kafka
implementation and its registry.

For Business Automation Insights Kubernetes deployment mode to support your custom events, you need to configure how the
events are routed from Kafka to Elasticsearch as explained
[here](https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/21.0.x?topic=events-event-forwarder)

This is the easiest way to run this sample with IBM Automation Foundation.

### IBM Automation Foundation Avro schema example

A schema is a structure in JSON format which is generally located in a `.avsc` file. Here is an example:

```json
{
  "name": "generic",
  "type": "record",
  "namespace": "com.ibm.bai",
  "fields": [
    {
      "name": "order",
      "type": "string"
    },
    {
      "name": "total_price",
      "type": "int"
    },
    {
      "name": "products",
      "type": {
        "type": "array",
        "items": {
          "name": "products_record",
          "type": "record",
          "fields": [
            {
              "name": "product_id",
              "type": "string",
              "ibm.automation.identifier": true
            },
            {
              "name": "description",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "quantity",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
```

NOTE: the `"ibm.automation.identifier": true` field attribute is optional and specific to IBM Business Automation
Insights.  
This attribute allows the user to create an identifier that can be used later for creating some monitoring sources.  
For more information, see the following documentation pages:

- [Business Automation Insights for server related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_config_monitsource.html)
- [Business Automation Insights for Kubernetes related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_21.0.x/com.ibm.dba.bai/topics/tsk_bai_k8s_cust_event_config_monitsource.html)

You can also find this schema [here](src/test/resources/iaf/generic-v1.0.0.avsc).

The Avro schema must be registered in an Avro registry. It is used to validate and encode events. As already detailed
in the Confluent related chapter, the
[management service client](src/main/java/com/ibm/dba/bai/avro/samples/ManagementServiceClient.java) client is in
charge of this purpose.
For more information, see the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).

### Corresponding IBM Automation Foundation event example

The following event complies with the Avro schema that is presented in the previous section:

```json
{
  "order": "13d478-36e",
  "total_price": 500,
  "products": [
    {
      "product_id": "First product",
      "description": null,
      "quantity": 1
    },
    {
      "product_id": "Second product",
      "description": {
        "string": "Fragile"
      },
      "quantity": 2
    }
  ]
}
```

You can find the corresponding file [here](src/test/resources/iaf/generic-event.json).

### Running the IBM Automation Foundation sample

1. Ensure that IBM Business Automation Insights is installed and IBM Automation Foundation is configured.
1. Edit the [iaf.config](iaf.config) file.
    - Set the topic name (important note: the topic name must start with `icp4ba-bai`):  
      `TOPIC=icp4ba-bai-ingress4samples-test`
    - Set the event source file: `EVENT=src/test/resources/iaf/generic-event.json`
    - Set the Kafka username of your Business Automation Insights installation. The [documentation](https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/21.0.x?topic=data-retrieving-kafka-username-password)
provides a way to retrieve this information.  
      `KAFKA_USERNAME=kafka_user`
    - Set the corresponding Kafka password of your Business Automation Insights installation:  
      `KAFKA_PASSWORD=aPassw0rd`
    - Set the path to a Kafka client security properties file:
      `KAFKA_SECURITY_PROPERTIES=src/test/resources/iaf/kafkaProducer.properties`
    - Set the path to the schema definition file:  
      `SCHEMA=src/test/resources/iaf/generic-v1.0.0.avsc`
    - Set the Business Automation Insights management service URL. The
host of the URL is the Management route.
      `MANAGEMENT_URL=https://bai-management-bai.apps.my-domain.com`
    - Set the management service registered username. The [documentation](https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/21.0.x?topic=indexes-rolling-over-that-store-active-summaries)
     provides a way to obtain this information.  
      `MANAGEMENT_USERNAME=adminUser`
    - Set the management service user password:  
      `MANAGEMENT_PASSWORD=user-password`

1. Edit the Kafka client properties file, for example: [kafkaProducer.properties](./src/test/resources/iaf/kafkaProducer.properties).
1. Compile the sample: `./gradlew clean jar`
1. Run the sample: `bin/run-iaf-sample`. This produces an output similar to [this example output](#example-output-with-iaf)
1. Check that the Elasticsearch index `icp4ba-bai-ingress4samples-test` (corresponding to the above topic) is created
in the IBM Automation Foundation environment:

  ```sh
  export ES_USER=admin
  export ES_PASSWORD=passw0rd
  export ES_CLIENT_POD=$(kubectl get pods -o custom-columns=Name:.metadata.name --no-headers --selector=chart=ibm-dba-ek,role=client)
  export ES_URL=https://localhost:9200
  kubectl exec -it ${ES_CLIENT_POD} -- curl -u ${ES_USER}:${ES_PASSWORD} -k ${ES_URL}/_cat/indices/icp4ba-bai-ingress4samples-test?h=index,docs.count\&v
  ```

#### Example output with IBM Automation Foundation

```text
KAFKA_USERNAME is icp4ba-kafka-auth
KAFKA_PASSWORD is **********
KAFKA_SECURITY_PROPERTIES is src/test/resources/iaf/KafkaProducer.properties
TOPIC is icp4ba-bai-testkit-custom-event
EVENT is src/test/resources/iaf/generic-event.json
SCHEMA is src/test/resources/iaf/generic-v1.0.0.avsc
MANAGEMENT_URL is https://management.bai.apps.bai-ocp-test.cp.fyre.ibm.com
MANAGEMENT_USERNAME is admin
MANAGEMENT_PASSWORD is **********
Using default value of "*********" for property name "sasl.jaas.config"
topics: [icp4ba-bai-1, icp4ba-bai-2, icp4ba-bai-ingress4samples-test]
topic 'icp4ba-bai-ingress4samples-test' already exist, not creating it
configuring topic 'icp4ba-bai-ingress4samples-test'
adding 120s message retention duration
Schema src/test/resources/iaf/generic-v1.0.0.avsc successfully registered
Sent event: {
  "order": "13d478-36e",
  "total_price": 500,
  "products": [
    {
      "product_id": "First product",
      "description": null,
      "quantity": 1
    },
    {
      "product_id": "Second product",
      "description": {
        "string": "Fragile"
      },
      "quantity": 2
    }
  ]
}

Kafka consumer listening for messages on topic 'icp4ba-bai-ingress4samples-test'
Received a message: offset: 0, partition: 11, key: null, value: {"order": "13d478-36e", "total_price": 500, "products":
 [{"product_id": "First product", "description": null, "quantity": 1}, {"product_id": "Second product", "description":
  "Fragile", "quantity": 2}]}
Found 1 message(s).
```

### Java code to send an event with IBM Automation Foundation

You send an event in three steps: ensure the schema is registered in the IBM Automation Foundation registry, convert
the event to binary and send this binary payload.

#### Procedure to register a schema with IBM Automation Foundation

Follow the procedure described in the [Java code to register a schema](#java-code-to-register-a-schema) section.

#### Convert the event JSON payload to binary representation by using a schema with IBM Automation Foundation

```java
GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema)
DecoderFactory decoderFactory = DecoderFactory.get();
Decoder decoder = decoderFactory.jsonDecoder(schema, jsonEvent);
Object encodedMessage = reader.read(null, decoder);
if (schema.getType().equals(Schema.Type.STRING)) {
  encodedMessage = ((Utf8) object).toString();
}
```

You can find this code in
[KafkaAvroProducerCommon](./src/main/java/com/ibm/dba/bai/avro/samples/KafkaAvroProducerCommon.java)
in the sample code.

#### Send an event as a binary payload with the IBM Automation Foundation API

```java
Properties props = ....;
// Get a new Generic KafkaProducer
KafkaProducer<String, Object> producer = new KafkaProducer<>(props);

// Read in the local schema file
Schema.Parser schemaDefinitionParser = new Schema.Parser();
Schema schema = schemaDefinitionParser.parse(new File(schemaPath));

// the string event is supposed to be conformal to the schema
Object eventObject = jsonToAvro(event, schema);

// Prepare the record
ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, eventObject);
// Send the record to Kafka

producer.send(producerRecord);

```

You can find this kind of code in
[IAFProducer.java](./src/main/java/com/ibm/dba/bai/avro/samples/iaf/IAFProducer.java).

## Tests

For development work, you can directly run event emission, for both supported platforms, from your preferred IDE.  
The [test source code](./src/test) contains a [single class](./src/test/java/tests/SampleTests.java) allowing you to test:

- event emission for IBM Automation Foundation
- event emission for Confluent
- schema registration
- sending event using the jar (once built and present in the `build/libs` directory)

The launch arguments are respectively read from
 [src/test/resources/tests/variables4Confluent.properties](./src/test/resources/tests/variables4Confluent.properties) or
 [src/test/resources/tests/variables4IAF.properties](./src/test/resources/tests/variables4IAF.properties)
 project resource properties files.  
These properties files respectively have to contain the exact same property names as the
[confluent.config](./confluent.config) or
 [iaf.config](./iaf.config)
configuration files that are used when the executable JAR file of the

 samples is launched.
