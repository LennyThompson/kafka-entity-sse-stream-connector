package org.acme.kafka;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.annotations.Broadcast;

import common.Common;
import entity.Entity;
import entity.Entity.EntityData;
import header.HeaderOuterClass;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.smallrye.mutiny.Multi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * A bean consuming data from the "prices" Kafka topic and applying some conversion.
 * The result is pushed to the "my-data-stream" stream which is an in-memory stream.
 */
@ApplicationScoped
public class EntityConverter 
{
    private final Logger LOGGER = Logger.getLogger(EntityConverter.class);

    // Consume from the `prices` channel and produce to the `my-data-stream` channel
    @Incoming("entity-data-quarkus")
    @Outgoing("entity-json-stream")
    // Send to all subscribers
    @Broadcast
    // Acknowledge the messages before calling this method.
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Multi<String> process(Entity.EntityMessage msgEntity) 
    {
        LOGGER.infov("Recieved entity message: {0}", msgEntity.getDataList().stream().map(data -> data.getId().getUuid().getValue()).collect(Collectors.joining(",")));

        return Multi.createFrom().items
        (
            msgEntity.getDataList().stream()
                .map
                (
                    data ->
                    {
                        return entityDataToJson(data);
                    }
                )
        );
        
   }

   private String entityDataToJson(Entity.EntityData entityData)
   {
        String strData = null;
        try
        {
            strData = JsonFormat.printer().preservingProtoFieldNames().print(entityData);
        }   
        catch(InvalidProtocolBufferException exc)
        {
            LOGGER.error("Invalid entity message", exc);
            strData = "{}";
        } 
        LOGGER.infov("**** EntityData = {0}", strData);
        return strData;
   }
}
