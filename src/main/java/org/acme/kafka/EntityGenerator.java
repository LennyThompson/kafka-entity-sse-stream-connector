package org.acme.kafka;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import common.Common;
import entity.Entity;
import entity.Entity.EntityData;
import header.HeaderOuterClass;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import kinematics.KinematicsOuterClass;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;
import position.PositionOuterClass;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.quarkus.runtime.StartupEvent;


@ApplicationScoped
public class EntityGenerator
{
    private static int NEXT_ID = 0;
    private static Random RANDOM_COORD_GENERATOR = new Random();
    private Map<Integer, EntityData> m_mapEntityTrack;
    private int m_nCurrEntityKey;

    private static final Logger LOGGER = Logger.getLogger(EntityGenerator.class);

    private int getNextId()
    {
        NEXT_ID++;
        return NEXT_ID;
    }

    public String nextUuid()
    {
        return String.format("Uuid:%09d", getNextId());
    }

    private double nextX()
    {
        return -2000.0 + (2000.0 - (-2000.0)) * RANDOM_COORD_GENERATOR.nextDouble();
    }

    private double nextY()
    {
        return -2000.0 + (2000.0 - (-2000.0)) * RANDOM_COORD_GENERATOR.nextDouble();
    }

    private double nextZ()
    {
        return 0.0;
    }

    private void initTrackMap()
    {
        AtomicInteger index = new AtomicInteger();
        m_mapEntityTrack = IntStream.range(1, 50)
            .mapToObj
                (
                    (rangeIndex) ->
                    {
                        String uuid = nextUuid();
                        return EntityData.newBuilder()
                            .setId(Common.Id.newBuilder().setName(uuid)
                            .setUuid(Common.Uuid.newBuilder().setValue(uuid).build()))
                            .setKinematics
                                (
                                    KinematicsOuterClass.Kinematics.newBuilder().setPosition
                                        (
                                            PositionOuterClass.Position.newBuilder().setCartesian
                                                (
                                                    PositionOuterClass.CartesianPosition.newBuilder().setX(nextX()).setY(nextY()).setZ(nextZ()).build()
                                                ).build()
                                        ).build()
                                )
                            .build();
                    }
                )
        .collect
            (
                Collectors.toMap
                (
                    data -> index.getAndIncrement(), data -> data
                )
            );
        LOGGER.infov("The number of entities in map: {0}", m_mapEntityTrack.size());
        m_nCurrEntityKey = 0;
    }

    private EntityData nextEntity()
    {
        if(m_mapEntityTrack == null)
        {
            initTrackMap();
        }

        if(m_nCurrEntityKey == m_mapEntityTrack.size())
        {
            m_nCurrEntityKey = 0;
        }
        EntityData entity = m_mapEntityTrack.get(m_nCurrEntityKey);
        Common.Uuid uuid = entity.getId().getUuid();
        m_mapEntityTrack.put
            (
                m_nCurrEntityKey,
                EntityData.newBuilder()
                    .setId(Common.Id.newBuilder().setName(uuid.getValue())
                    .setUuid(uuid))
                    .setKinematics
                    (
                        KinematicsOuterClass.Kinematics.newBuilder().setPosition
                            (
                                PositionOuterClass.Position.newBuilder().setCartesian
                                    (
                                        PositionOuterClass.CartesianPosition.newBuilder().setX(nextX()).setY(nextY()).setZ(0.0).build()
                                    ).build()
                            ).build()
                    )
                    .build());
        ++m_nCurrEntityKey;
        return entity;
    }

    public Entity.EntityMessage nextEntityMessage()
    {
        String key = nextUuid();
        Common.Id idService = Common.Id.newBuilder()
            .setName("service")
            .setUuid(Common.Uuid.newBuilder().setValue("uuid:000000999").build())
            .build();
        Common.Id idSystem = Common.Id.newBuilder()
            .setName("system")
            .setUuid(Common.Uuid.newBuilder().setValue("uuid:000000998").build())
            .build();
        HeaderOuterClass.Header header = HeaderOuterClass.Header.newBuilder()
            .setService(idService)
            .setSystem(idSystem)
            .build();

        Entity.EntityMessage.Builder buildEntity = Entity.EntityMessage.newBuilder();
        buildEntity
            .setHeader(header);
        IntStream.range(1, 10)
            .forEach
                (
                    (index) ->
                    {
                        buildEntity.addData(nextEntity());
                    }
                );

        return buildEntity.build();
    }

    void onStart(@Observes StartupEvent ev) 
    {               
        LOGGER.info("Initialising the entity map");
        initTrackMap();
    }


    @Outgoing("generated-entity-data-quarkus")
    public Multi<Record<String, Entity.EntityMessage>> generateEntities()
    {
        return Multi.createFrom().ticks()
            .every(Duration.ofMillis(500))
            .onOverflow().drop()
            .map
            (
                tick ->
                {
                    String strUuid = nextUuid();
                    Entity.EntityMessage msgEntity = nextEntityMessage();
                    LOGGER.infov("entity: {0}, tracking: {1}", strUuid, msgEntity.getDataList().stream().map(data -> data.getId().getUuid().getValue()).collect(Collectors.joining(",")));
                    return Record.of(nextUuid(), msgEntity);
                }
            );
    }

    @Outgoing("tracked-entities")
    public Multi<Record<Integer, String>> trackedEntities()
    {
        if(m_mapEntityTrack == null)
        {
            initTrackMap();
        }

        return Multi.createFrom().items
        (
            m_mapEntityTrack.entrySet().stream()
            .map
            (
                entry -> Record.of
                (
                    entry.getKey(),
                    "{ \"id\" : "
                        + entry.getValue().getId().getUuid()
                        + ", \"position\" : { \"x\": "
                        + entry.getValue().getKinematics().getPosition().getCartesian().getX()
                        + ", \"y\": "
                        + entry.getValue().getKinematics().getPosition().getCartesian().getY()
                        + ", \"z\": "
                        + entry.getValue().getKinematics().getPosition().getCartesian().getZ()
                        + "} }"
                )
            )
        );
    }
}

