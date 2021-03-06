package org.air6500.kafka;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.jboss.resteasy.annotations.SseElementType;
import org.reactivestreams.Publisher;

import org.eclipse.microprofile.reactive.messaging.Channel;

/**
 * A simple resource retrieving the "in-memory" "my-data-stream" and sending the items to a server sent event.
 */
@Path("/entity")
public class EntityResource 
{
    @Inject
    @Channel("entity-json-stream")
    Publisher<String> entities;

    @GET
    @Path("/data")
    @Produces(MediaType.SERVER_SENT_EVENTS) // denotes that server side events (SSE) will be produced
    @SseElementType("application/json") // denotes that the contained data, within this SSE, is just regular text/plain data
    public Publisher<String> stream() 
    {
        return entities;
    }
}
