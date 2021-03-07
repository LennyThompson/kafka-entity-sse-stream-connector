package org.air6500.kafka;

import javax.enterprise.context.ApplicationScoped;

import kinematics.KinematicsOuterClass;
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
import position.PositionOuterClass;

/**
 * A bean consuming data from the "prices" Kafka topic and applying some conversion.
 * The result is pushed to the "my-data-stream" stream which is an in-memory stream.
 */
@ApplicationScoped
public class EntityConverter 
{
    private final Logger LOGGER = Logger.getLogger(EntityConverter.class);
    private final static double SCALE =  2000.0 / 40000.0;
    private final static double XOFFSET = 2000.0;
    private final static double YOFFSET = 2000.0;

    // Consume from the `prices` channel and produce to the `my-data-stream` channel
    @Incoming("entity-entity")
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
                        return entityDataToJson(transformData(data));
                    }
                )
        );
    }

    protected String entityDataToJson(Entity.EntityData entityData)
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

    protected Entity.EntityData transformData(EntityData entityData)
    {
       if(entityData.getKinematics().getPosition().hasCartesian())
       {
           return entityData;
       }
       if(entityData.getKinematics().getPosition().hasPolar())
       {
           double dX = entityData.getKinematics().getPosition().getPolar().getRange() * Math.sin(entityData.getKinematics().getPosition().getPolar().getAzimuth());
           double dY = entityData.getKinematics().getPosition().getPolar().getRange() * Math.cos(entityData.getKinematics().getPosition().getPolar().getAzimuth());
           dX = SCALE * dX + XOFFSET;
           dY = SCALE * dY+ YOFFSET;

           return EntityData.newBuilder()
               .setTimestamp(entityData.getTimestamp())
               .setId(entityData.getId())
               .setKinematics
                   (
                       KinematicsOuterClass.Kinematics.newBuilder().setPosition
                           (
                               PositionOuterClass.Position.newBuilder().setCartesian(PositionOuterClass.CartesianPosition.newBuilder().setX(dX).setY(dY).setZ(0.0).build())
                           ).build()

               ).build();
        }
        else if(entityData.getKinematics().getPosition().hasSpherical())
        {
            PositionOuterClass.SphericalPosition posSpherical = entityData.getKinematics().getPosition().getSpherical();
            double dHoriz = posSpherical.getRange() * Math.cos(posSpherical.getElevation()) * SCALE;
            double dX = dHoriz * Math.sin(posSpherical.getAzimuth())  + XOFFSET;
            double dY = dHoriz * Math.cos(posSpherical.getAzimuth()) + YOFFSET;
            double dZ = posSpherical.getRange() * Math.sin(posSpherical.getElevation());

            return EntityData.newBuilder()
                .setTimestamp(entityData.getTimestamp())
                .setId(entityData.getId())
                .setKinematics
                    (
                        KinematicsOuterClass.Kinematics.newBuilder().setPosition
                            (
                                PositionOuterClass.Position.newBuilder().setCartesian(PositionOuterClass.CartesianPosition.newBuilder().setX(dX).setY(dY).setZ(0.0).build())
                            ).build()

                    ).build();
        }
        else if(entityData.getKinematics().getPosition().hasCartesian())
        {
            return entityData;
        }
        return null;
    }
}
