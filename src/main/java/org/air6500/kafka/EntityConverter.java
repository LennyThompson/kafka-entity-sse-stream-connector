package org.air6500.kafka;

import javax.enterprise.context.ApplicationScoped;

import covariance.CovarianceOuterClass;
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

    @Incoming("entity-entity")
    @Outgoing("entity-json-stream")
    // Send to all subscribers
    @Broadcast
    // Acknowledge the messages before calling this method.
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Multi<String> processEntity(Entity.EntityMessage msgEntity)
    {
        LOGGER.infov("Received entity message: {0}", msgEntity.getDataList().stream().map(data -> data.getId().getUuid().getValue()).collect(Collectors.joining(",")));

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

    @Incoming("entity-plot")
    @Outgoing("plot-json-stream")
    // Send to all subscribers
    @Broadcast
    // Acknowledge the messages before calling this method.
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Multi<String> processPlot(Entity.EntityMessage msgEntity)
    {
        LOGGER.infov("Received entity message: {0}", msgEntity.getDataList().stream().map(data -> data.getId().getUuid().getValue()).collect(Collectors.joining(",")));

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

    @Incoming("entity-track")
    @Outgoing("track-json-stream")
    // Send to all subscribers
    @Broadcast
    // Acknowledge the messages before calling this method.
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Multi<String> processTrack(Entity.EntityMessage msgEntity)
    {
        LOGGER.infov("Received entity message: {0}", msgEntity.getDataList().stream().map(data -> data.getId().getUuid().getValue()).collect(Collectors.joining(",")));

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
        try
        {
            LOGGER.infov("Input entity message - {0}", JsonFormat.printer().preservingProtoFieldNames().print(entityData));
        }
        catch (InvalidProtocolBufferException e)
        {
            LOGGER.error("Error in json serialisation: ", e);
        }

        if(entityData.getKinematics().getPosition().hasCartesian())
        {
            double dX = SCALE * entityData.getKinematics().getPosition().getCartesian().getX() + XOFFSET;
            double dY = SCALE * entityData.getKinematics().getPosition().getCartesian().getY() + YOFFSET;

            return EntityData.newBuilder()
                .setTimestamp(entityData.getTimestamp())
                .setId(entityData.getId())
                .setKinematics
                    (
                        KinematicsOuterClass.Kinematics.newBuilder().setPosition
                            (
                                PositionOuterClass.Position.newBuilder().setCartesian(PositionOuterClass.CartesianPosition.newBuilder().setX(dX).setY(dY).setZ(0.0).build())
                            )
                            .setPositionCovarianceSqrt
                            (
                                CovarianceOuterClass.CovarianceSqrt.newBuilder()
                                    .setR1C1(entityData.getKinematics().getPositionCovarianceSqrt().getR1C1())
                                    .setR2C1(entityData.getKinematics().getPositionCovarianceSqrt().getR2C1())
                                    .setR2C2(entityData.getKinematics().getPositionCovarianceSqrt().getR2C2())
                                    .setR3C1(entityData.getKinematics().getPositionCovarianceSqrt().getR3C1())
                                    .setR3C2(entityData.getKinematics().getPositionCovarianceSqrt().getR3C2())
                                    .setR3C3(entityData.getKinematics().getPositionCovarianceSqrt().getR3C3())
                            ).build()

                    ).build();
        }
        else if(entityData.getKinematics().getPosition().hasPolar())
        {
            double dX = entityData.getKinematics().getPosition().getPolar().getRange() * Math.sin(entityData.getKinematics().getPosition().getPolar().getAzimuth());
            double dY = entityData.getKinematics().getPosition().getPolar().getRange() * Math.cos(entityData.getKinematics().getPosition().getPolar().getAzimuth());
            dX = SCALE * dX + XOFFSET;
            dY = SCALE * dY + YOFFSET;

            return EntityData.newBuilder()
               .setTimestamp(entityData.getTimestamp())
               .setId(entityData.getId())
               .setKinematics
                   (
                       KinematicsOuterClass.Kinematics.newBuilder().setPosition
                           (
                               PositionOuterClass.Position.newBuilder().setCartesian(PositionOuterClass.CartesianPosition.newBuilder().setX(dX).setY(dY).setZ(0.0).build())
                           )
                           .setPositionCovarianceSqrt
                           (
                               CovarianceOuterClass.CovarianceSqrt.newBuilder()
                                   .setR1C1(entityData.getKinematics().getPositionCovarianceSqrt().getR1C1())
                                   .setR2C1(entityData.getKinematics().getPositionCovarianceSqrt().getR2C1())
                                   .setR2C2(entityData.getKinematics().getPositionCovarianceSqrt().getR2C2())
                                   .setR3C1(entityData.getKinematics().getPositionCovarianceSqrt().getR3C1())
                                   .setR3C2(entityData.getKinematics().getPositionCovarianceSqrt().getR3C2())
                                   .setR3C3(entityData.getKinematics().getPositionCovarianceSqrt().getR3C3())
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
                            )
                            .setPositionCovarianceSqrt
                            (
                                CovarianceOuterClass.CovarianceSqrt.newBuilder()
                                    .setR1C1(entityData.getKinematics().getPositionCovarianceSqrt().getR1C1())
                                    .setR2C1(entityData.getKinematics().getPositionCovarianceSqrt().getR2C1())
                                    .setR2C2(entityData.getKinematics().getPositionCovarianceSqrt().getR2C2())
                                    .setR3C1(entityData.getKinematics().getPositionCovarianceSqrt().getR3C1())
                                    .setR3C2(entityData.getKinematics().getPositionCovarianceSqrt().getR3C2())
                                    .setR3C3(entityData.getKinematics().getPositionCovarianceSqrt().getR3C3())
                            ).build()


                    ).build();
        }
        return null;
    }
}
