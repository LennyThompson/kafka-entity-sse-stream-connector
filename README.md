# Kafka Entity SSE Stream Connector

Simple quarkus kafka connector that sinks a stream of messages from kafka topic(s) transforms them to EntityData with Cartesian Position data and streams the result through server side events to http client.

The quarkus framework provides almost all of teh infrastructure, all that is coded here is the dodgy to cartesian transform and something to send the event stream.

Note that without a subscription to the event stream nothing will happen.
Note also that the quarkus framework will only provide the latest content from the topic(s).


