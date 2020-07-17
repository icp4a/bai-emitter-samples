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
  - [Event example](#event-example)
  - [Running the sample](#running-the-sample)
    - [Example output](#example-output)
  - [Java code to register a schema](#java-code-to-register-a-schema)
  - [Java code to send an event](#java-code-to-send-an-event)
    - [Get a schema from the registry](#get-a-schema-from-the-registry)
    - [Convert the event JSON payload to binary representation by using a schema](#convert-the-event-json-payload-to-binary-representation-by-using-a-schema)
    - [Send an event as a binary payload](#send-an-event-as-a-binary-payload)
- [Event Streams](#event-streams)
  - [Avro schema example](#avro-schema-example)
  - [Corresponding event example](#corresponding-event-example)
  - [Running the Event Streams sample](#running-the-event-streams-sample)
    - [Example output with Event Streams](#example-output-with-event-streams)
  - [Procedure to register a schema](#procedure-to-register-a-schema)
  - [Java code to send an event with Event Streams](#java-code-to-send-an-event-with-event-streams)
    - [Convert the event JSON payload to binary representation by using a schema with Event Streams](#convert-the-event-json-payload-to-binary-representation-by-using-a-schema-with-event-streams)
    - [Send an event as a binary payload with the Event Streams APIs](#send-an-event-as-a-binary-payload-with-the-event-streams-api)

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
##      elasticsearch-index: <Elasticseach index name 1>
##      hdfs-bucket: <hdfs://hdfs-hostname/bucket-name 1>
##    - kafka-topic: <topic name 2>
##      elasticsearch-index: <Elasticseach index name 2>
---
configurations:
    - kafka-topic: CUSTOMER_V1
      elasticsearch-index: bai
```

In this example, Business Automation Insights forwards events from the `CUSTOMER_V1` Kafka topic to the `bai`
Elasticsearch index.

### Schema example

A schema is a structure in JSON format which is generally located in a `.avsc` file. Here is an example:

```json
{
  "namespace": "com.ibm.customers",
  "type": "record",
  "name": "Customer",
  "fields": [
    {
      "name": "company",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    }
  ]
}
```

You can also find this schema [here](src/test/resources/confluent/CUSTOMER_V1.avsc).

The Avro schema must be registered in an Avro registry. It is used to validate and encode events.
For more information, see the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).

### Event example

The following event complies with the Avro schema that is presented in the previous section:

```json
{
  "company": "IBM",
  "address": "Everywhere in the world"
}
```

You can find the corresponding file [here](src/test/resources/avro-sample-event.json).

### Running the sample

1. Start Business Automation Insights for a server.
2. Edit the [confluent.config](confluent.config) file.
    - Set the topic name: `TOPIC=CUSTOMER_V1`
    - Set the Kafka credentials: `KAFKA_USERNAME=<kafka_user>` and `KAFKA_PASSWORD=<kafka_password>`
    - Set the event source file: `EVENT=@src/test/resources/avro-sample-event.json`
    - Set the schemas configuration file: `@./src/test/resources/confluent/TestSchemas.properties`
    - Set the path to a Kafka security properties file:
    `KAFKA_SECURITY_PROPERTIES=src/test/resources/confluent/KafkaAvroProducer.properties`
3. Edit the Kafka security properties file, example:
  [kafka-security.properties](./src/test/resources/confluent/KafkaAvroProducer.properties)
4. Compile the sample: `./gradlew clean jar`
5. Run the sample: `bin/run-confluent-sample`. This produces an output similar to
  [this example output](#example-output)
6. To verify that the event sent with the sample is processed by Business Automation Insights, check that the
  Elasticsearch index `bai` is created, for example:
  `curl -X GET -u <admin>:<admin_password> -k https://localhost:9200/_cat/indices/bai?h=index,docs.count\&v`

#### Example output

```text
KAFKA_SECURITY_PROPERTIES is bai-4s-kafka.properties
REGISTRY_URL is https://localhost:8084
SCHEMAS is @./src/test/resources/confluent/TestSchemas.properties
KAFKA_USERNAME is admin
KAFKA_PASSWORD is **********
TOPIC is CUSTOMER_V1
EVENT is @src/test/resources/avro-sample-event.json
loading the schema mappings from ./src/test/resources/confluent/TestSchemas.properties
Deleting all existing schemas in the registry
Registered subjects are: ["BASIC_SCHEMA_V3-value","CUSTOMER_V3-value","CUSTOMER_V1-value","BASIC_SCHEMA_V2-value","BASIC_SCHEMA_V1-value","CUSTOMER_V2-value"]
Deleted all schemas under subject BASIC_SCHEMA_V3-value :[3]
Deleted all schemas under subject CUSTOMER_V3-value :[3]
Deleted all schemas under subject CUSTOMER_V1-value :[3]
Deleted all schemas under subject BASIC_SCHEMA_V2-value :[3]
Deleted all schemas under subject BASIC_SCHEMA_V1-value :[3]
Deleted all schemas under subject CUSTOMER_V2-value :[3]
Registered subjects are: []
Default top level compatibility set to:
    compatibility : forward
Registering schemas ...
Registered new version of BASIC_SCHEMA_V3-value:
    id : 1
Registered new version of BASIC_SCHEMA_V1-value:
    id : 2
Registered new version of BASIC_SCHEMA_V2-value:
    id : 3
Registered new version of CUSTOMER_V3-value:
    id : 1
Registered new version of CUSTOMER_V1-value:
    id : 2
Registered new version of CUSTOMER_V2-value:
    id : 3
Registered subjects are: ["BASIC_SCHEMA_V3-value","CUSTOMER_V3-value","CUSTOMER_V1-value","BASIC_SCHEMA_V2-value","BASIC_SCHEMA_V1-value","CUSTOMER_V2-value"]
Latest version registered under subject CUSTOMER_V1-value is:
    subject : CUSTOMER_V1-value
    version : 4
    id : 2
    schema : {"type":"record","name":"Customer","namespace":"com.ibm.customers","fields":[{"name":"company","type":"string"},{"name":"address","type":"string"}]}
Record sent: {"company": "IBM France", "address": "Hybrid Cloud Lane"}
```

### Java code to register a schema

You upload a schema to the Confluent Avro registry by using a name as reference. This name defines both the
 subject that is used to retrieve a schema from the registry **and** the Kafka topic used to send an event.
 The relationship between the subject and the topic is described here:
 [What is a topic versus a schema versus a subject?](https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html#terminology-review)

This sample uses the
[Confluent default name strategy](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#sr-schemas-subject-name-strategy)
where the topic name is the name that was used at schema registration time. The subject name is deduced from the topic
name by adding a `-value` keyword to the topic name.

The code of the sample uses the Confluent REST API in the `registerNewSchemaVersion` method of the
[ConfluentAvroRegistry class](./src/main/java/com/ibm/dba/bai/avro/samples/confluent/ConfluentAvroRegistry.java).
You can find the Confluent registry Rest API reference for uploading a new schema
[here](https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions)

```java
    // Content of the src/test/resources/confluent/CUSTOMER_V1.avsc file
    String jsonSchema = ...;
    okhttp3.MediaType AVRO_JSON_MEDIA_TYPE = okhttp3.MediaType.parse("application/vnd.schemaregistry.v1+json");
    okhttp3.Request request = new okhttp3.Request.Builder()
      .post(okhttp3.RequestBody.create( jsonSchema, AVRO_JSON_MEDIA_TYPE))
      .url(avroEndpoint + "/subjects/" + schemaSubject + "/versions")
      .build();
    String result = client.newCall(request).execute().body().string();
```

### Java code to send an event

Sending an event requires three steps: retrieve a schema, convert the event to binary, send this binary payload.

#### Get a schema from the registry

```java
// Concatenate the subject to the Avro schema registry API endpoint
String endpoint = avroEndpoint + "/subjects/" + subject + "/versions/latest";
Request request = new Request.Builder().url(endpoint).build();
String jsonResponse = httpClient.newCall(request).execute().body().string();
// Extract the needed fields ("subject", "schema", etc.) from the response
Map<String, Object> map = (Map<String, Object>) Json.parseJson(jsonResponse);
String schemaDefinition = (String) map.get("schema");
Schema.Parser schemaParser = new Schema.Parser().setValidate(false).setValidateDefaults(false);
Schema schema = schemaParser.parse(schemaDefinition);
```

You can find this code in
[ConfluentAvroRegistry](./src/main/java/com/ibm/dba/bai/avro/samples/confluent/ConfluentAvroRegistry.java)
in the sample code.

#### Convert the event JSON payload to binary representation by using a schema

```java
Schema schema = ...;
String jsonEvent = ...;
DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(jsonEvent.getBytes()));
Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataIn);
DatumReader<Object> reader = new GenericDatumReader<>(schema);
Object datum = reader.read(null, decoder);
GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
ByteArrayOutputStream os = new ByteArrayOutputStream();
Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
writer.write(datum, encoder);
encoder.flush();
byte[] encodedMessage = os.toByteArray();
```

You can find this code in
[KafkaAvroProducerCommon](./src/main/java/com/ibm/dba/bai/avro/samples/KafkaAvroProducerCommon.java)
in the sample code.

#### Send an event as a binary payload

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

You can find this code in [ConfluentKafkaAvroProducer](./src/main/java/com/ibm/dba/bai/avro/samples/confluent/ConfluentKafkaAvroProducer.java).

## Event Streams

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
              "type": "string"
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

You can also find this schema [here](src/test/resources/eventstream/generic-v1.0.0.avsc).

The Avro schema must be registered in an Avro registry. It is used to validate and encode events.
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
    - Set the topic name: `TOPIC=bai-ingress`
    - Set the event source file: `EVENT=@src/test/resources/eventstream/generic-event.json`
    - Set the path to a Kafka client properties file:
    `KAFKA_CLIENT_PROPERTIES=src/test/resources/eventstream/kafkaProducer.properties`
    - Set the path to an Event Streams configuration properties file:
    `EVENT_STREAMS_PROPERTIES=src/test/resources/eventstream/eventStream.properties`
    - Set the schema name and version, using the same values as for the
    [registered schema](#procedure-to-register-a-schema), as decribed in the next section:
    `SCHEMA_NAME=generic` and `SCHEMA_VERSION=1.0.0`

1. Edit the Kafka client properties file, example: [kafka-client-config.properties](./src/test/resources/eventstream/kafkaProducer.properties)
1. Edit the Event Streams configuration properties file, example: [eventstreams-config.properties](./src/test/resources/eventstream/eventStream.properties)
1. Compile the sample: `./gradlew clean jar`
1. Run the sample: `bin/run-eventstreams-sample`. This produces an output similar to [this example output](#example-output-with-event-streams)
1. Check that the Elasticsearch index `bai` is created in the Event Streams environment:

  ```sh
  export ES_USER=admin
  export ES_PASSWORD=passw0rd
  export ES_CLIENT_POD=$(kubectl get pods -o custom-columns=Name:.metadata.name --no-headers --selector=chart=ibm-dba-ek,role=client)
  export ES_URL=https://localhost:9200
  kubectl exec -it ${ES_CLIENT_POD} -- curl -u ${ES_USER}:${ES_PASSWORD} -k ${ES_URL}/_cat/indices/bai?h=index,docs.count\&v
  ```

#### Example output with Event Streams

```text
Connecting to the schema registry
Retrieving the schema with name = generic and version = 1.0.0
Found a schema in the registry: {"type":"record","name":"generic","namespace":"com.ibm.bai","fields":[{"name":"order","type":"string"},{"name":"total_price","type":"int"},{"name":"products","type":{"type":"array","items":{"type":"record","name":"products_record","fields":[{"name":"product_id","type":"string"},{"name":"description","type":["string","null"]},{"name":"quantity","type":"int"}]}}}]}
Sent event: { "order": "13d478-36e", "total_price": 500, "products":
  [
    { "product_id": "First product", "description": null, "quantity": 1 },
    { "product_id": "Second product", "description": { "string": "Fragile" }, "quantity": 2 }
  ]
}
Kafka consumer listening for messages on topic bai-ingress
Received a message: offset: 0, partition: 22, key: null, value: {"order": "13d478-36e", "total_price": 500, "products":
[{"product_id": "First product", "description": null, "quantity": 1}, {"product_id": "Second product", "description":
"Fragile", "quantity": 2}]}
```

### Procedure to register a schema

Follow the procedure described in the
[IBM Business Automation Insights Knowledge Center](https://www.ibm.com/supportknowledgecenter/SSYHZ8_20.0.x/com.ibm.dba.bai/topics/tsk_bai_k8s_cust_event_proc.html).
You must ensure that the name and version of the registered schema match exactly those configured in the
[previous section](#running-the-event-streams-sample).

### Java code to send an event with Event Streams

You send an event in two steps: convert the event to binary, send this binary payload.

#### Convert the event JSON payload to binary representation by using a schema with Event Streams

```java
DatumReader<Object> reader = new SpecificDatumReader<>(schema);
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
// specify BINARY encoding
pros.put("com.ibm.eventstreams.schemaregistry.encoding", "BINARY");

try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
  Schema schema = ...;
  Object encodedMessage = jsonToAvro(this.event, schemaValue);

  // Prepare the record, adding the Schema Registry headers
  ProducerRecord<String, Object> record = new ProducerRecord<>(topic, encodedMessage);
  record.headers().add(SchemaRegistryConfig.HEADER_MSG_SCHEMA_ID, schema.getIdAsBytes());
  record.headers().add(SchemaRegistryConfig.HEADER_MSG_SCHEMA_VERSION, schema.getVersionAsBytes());

  // Send the record to Kafka
  producer.send(producerRecord);
}
```

You can find this code in [EventStreamProducer](./src/main/java/com/ibm/dba/bai/avro/samples/eventstream/EventStreamProducer.java).
