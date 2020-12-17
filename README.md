# Samples for IBM Business Automation Insights custom event emitter

This directory provides samples related to using custom events with Business Automation Insights.
The samples demonstrate how to:

- Register an Avro schema.
- Send to Business Automation Insights events that conform to the registered schema, by using either Confluent or
  IBM Event Streams Avro APIs.

Before sending events, you must configure Business Automation Insights to process custom events.
For more information about such configuration, see the
[IBM Knowledge Center](https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/con_bai_custom_events.html).

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
- [Event Streams](#event-streams)
  - [Avro schema example](#avro-schema-example)
  - [Corresponding event example](#corresponding-event-example)
  - [Running the Event Streams sample](#running-the-event-streams-sample)
    - [Example output with Event Streams](#example-output-with-event-streams)
  - [Java code to send an event with Event Streams](#java-code-to-send-an-event-with-event-streams)
    - [Procedure to register a schema](#procedure-to-register-a-schema)
    - [Convert the event JSON payload to binary representation by using a schema with Event Streams](#convert-the-event-json-payload-to-binary-representation-by-using-a-schema-with-event-streams)
    - [Send an event as a binary payload with the Event Streams API](#send-an-event-as-a-binary-payload-with-the-event-streams-api)

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
  [kafka-security.properties](./src/test/resources/confluent/KafkaAvroProducer.properties)
- Confluent Kafka 5.4.1
- an Avro schema as an `.avsc` file (see the [schema example](#schema-example) below)
- an event to send, as a file in JSON format (`.json` file) corresponding to the Avro schema

## Confluent

IBM Business Automation Insights for a Server allows you to run the Confluent Kafka and Schema Registry containers.
This is the easiest way to run this sample with Confluent.

For Business Automation Insights to support your custom events, you need to configure how the events are routed from
Kafka to Elasticsearch and, optionally, to HDFS by modifying the `config/flink/event-processor-config.yml`
configuration file, as explained
[here](https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_proc.html)

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
In this sample, we considered a best practice to use the same value for
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

- [BAI for server related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_config_monitsource.html)
- [BAI for Kubernetes related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_k8s_cust_event_config_monitsource.html)

You can also find this schema [here](src/test/resources/confluent/generic/generic-schema.avsc).

### Schema registration

The Avro schema must be registered in an Avro registry. It is used to validate and encode events.
You do not need to directly access the schema registry to register the schema since this registration is the purpose  
of the management service.  
For more information about the schema specification, see the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).

### Event example

The following event complies with the Avro schema that is presented in the previous section:

```json
{
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
    - Set the kafka registry URL: `REGISTRY_URL=https://localhost:8084`
    - Set the event source file: `EVENT=src/test/resources/avro-sample-event.json`
    - Set the schema source file: `src/test/resources/confluent/generic/generic-schema.avsc`
    - Set the path to a Kafka security properties file:
    `KAFKA_SECURITY_PROPERTIES=src/test/resources/confluent/KafkaAvroProducer.properties`
3. Edit the Kafka security properties file, example:
  [kafka-security.properties](./src/test/resources/confluent/KafkaAvroProducer.properties)
4. Compile the sample: `./gradlew clean jar`
5. Run the sample: `bin/run-confluent-sample`. This produces an output similar to
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
Received a message: offset: 0, partition: 26, key: null, value: {"order": "3d478-36e", "total_price": 500,
"products": [{"product_id": "First product", "description": null, "quantity": 1}, {"product_id": "Second product",
"description": "Fragile", "quantity": 2}]}
1 message(s) found

```

### Java code to send an event

Sending an event requires four steps: retrieve a schema, ensure the schema registration, convert the event to binary
and send this binary payload.

#### Retrieve a schema

You can either retrieve a schema from a schema registry or by reading a schema from a `.avsc` file.
Retrieving a schema from the schema registry is not a feature that is used in the present samples because the schema
 definition is already known.  

#### Java code to register a schema

You upload a schema to the Confluent Avro registry thanks to the management service.  
 The [management service](./src/main/java/com/ibm/dba/bai/avro/samples/ManagementServiceClient.java)
 needs an Elasticsearch index name, a schema name, and the schema itself as JSON string.  
 This schema name defines both the subject used to retrieve a schema from the registry **and** the Kafka topic used to
 send an event.  
 Since the topic name is stored under an Elasticsearch index, a good naming convention is to use the same
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

## Event Streams

IBM Business Automation Insights Kubernetes version allows you to run the IBM Event Streams Kafka implementation and its
registry.

This is the easiest way to run this sample with IBM Event Streams.

### Avro schema example

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

- [BAI for server related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_sn_cust_event_config_monitsource.html)
- [BAI for Kubernetes related documentation]( https://www.ibm.com/support/knowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_k8s_cust_event_config_monitsource.html)

You can also find this schema [here](src/test/resources/eventstream/generic-v1.0.0.avsc).

The Avro schema must be registered in an Avro registry. It is used to validate and encode events. As already detailed in
 the Confluent related chapter, the
 [management service client](src/main/java/com/ibm/dba/bai/avro/samples/ManagementServiceClient.java) client is in
 charge of this purpose.
For more information, see the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).

### Corresponding event example

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

You can find the corresponding file [here](src/test/resources/eventstream/generic-event.json).

### Running the Event Streams sample

1. Ensure that IBM Business Automation Insights is installed and IBM Event Streams is configured.
1. Edit the [eventstreams.config](eventstreams.config) file.
    - Set the topic name: `TOPIC=bai-ingress4samples-test`
    - Set the event source file: `EVENT=src/test/resources/eventstream/generic-event.json`
    - Set the path to a Kafka client properties file:
    `KAFKA_CLIENT_PROPERTIES=src/test/resources/eventstream/kafkaProducer.properties`
    - Set the path to an Event Streams configuration properties file:
    `EVENT_STREAMS_PROPERTIES=src/test/resources/eventstream/eventStream.properties`
    - Set the path to the schema definition file:  
    `SCHEMA=src/test/resources/eventstream/generic-v1.0.0.avsc`
    - Set the management service url:  
    `MANAGEMENT_URL=https://localhost:6898`
    - Set the management service registered username:  
    `MANAGEMENT_USERNAME=adminUser`
    - Set the management service user password:  
    `MANAGEMENT_URL=user-password`

1. Edit the Kafka client properties file, example: [kafka-client-config.properties](./src/test/resources/eventstream/kafkaProducer.properties)
1. Edit the Event Streams configuration properties file, example: [eventstreams-config.properties](./src/test/resources/eventstream/eventStream.properties)
1. Compile the sample: `./gradlew clean jar`
1. Run the sample: `bin/run-eventstreams-sample`. This produces an output similar to [this example output](#example-output-with-event-streams)
1. Check that the Elasticsearch index `bai-ingress4samples-test` (corresponding to the above topic) is created in the
    Event Streams environment:

  ```sh
  export ES_USER=admin
  export ES_PASSWORD=passw0rd
  export ES_CLIENT_POD=$(kubectl get pods -o custom-columns=Name:.metadata.name --no-headers --selector=chart=ibm-dba-ek,role=client)
  export ES_URL=https://localhost:9200
  kubectl exec -it ${ES_CLIENT_POD} -- curl -u ${ES_USER}:${ES_PASSWORD} -k ${ES_URL}/_cat/indices/bai-ingress4samples-test?h=index,docs.count\&v
  ```

#### Example output with Event Streams

```text
EVENT_STREAMS_PROPERTIES is src/test/resources/eventstream/eventStream.properties
KAFKA_CLIENT_PROPERTIES is src/test/resources/eventstream/kafkaProducer.properties
TOPIC is ocp-ads-generic-event-sample-test
EVENT is src/test/resources/eventstream/generic-event.json
SCHEMA is src/test/resources/eventstream/generic-v1.0.0.avsc
MANAGEMENT_URL is https://management.bai.apps.ocp-ads.os.fyre.ibm.com
MANAGEMENT_USERNAME is admin
MANAGEMENT_PASSWORD is **********
Schema src/test/resources/eventstream/generic-v1.0.0.avsc successfully registered
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

Kafka consumer listening for messages on topic 'ocp-ads-generic-event-sample-test'
Received a message: offset: 0, partition: 1, key: null, value: {"order": "13d478-36e", "total_price": 500, "products":
 [{"product_id": "First product", "description": null, "quantity": 1}, {"product_id": "Second product", "description":
 "Fragile", "quantity": 2}]}
Found 1 messages.

```

### Java code to send an event with Event Streams

You send an event in three steps: ensure the schema is registered in the events stream registry, convert the event to
 binary and send this binary payload.

#### Procedure to register a schema

Follow the procedure described in the [Java code to register a schema](#java-code-to-register-a-schema) section.

#### Convert the event JSON payload to binary representation by using a schema with Event Streams

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

#### Send an event as a binary payload with the Event Streams API

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
 [EventStreamProducer.java](./src/main/java/com/ibm/dba/bai/avro/samples/eventstream/EventStreamProducer.java).

### Tests

For development work, you can directly run event emission, for both supported platforms, from your preferred IDE.  
The [test source code](./src/test) contains a [single class](./src/test/java/tests/SampleTests.java) allowing you to test:

- event emission for IBM Event Streams
- event emission for Confluent
- schema registration
- sending event using the jar (once built and present in the `build/libs` directory)

The launch arguments are respectively read from [src/test/resources/tests/variables4Confluent.properties](./src/test/resources/tests/variables4Confluent.properties)
 and [src/test/resources/tests/variables4EventStream.properties](./src/test/resources/tests/variables4EventStream.properties)
 project resource properties files.  
These properties files respectively have to contain the exact same property names as the [confluent.config](./confluent.config)
 and [eventstreams.config](./eventstreams.config) configuration files that are used when the executable JAR file of the
 samples is launched.
