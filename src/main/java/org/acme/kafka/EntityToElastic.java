package org.acme.kafka;

import java.lang.invoke.InjectedProfile;

import javax.inject.Inject;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

@ApplicationScoped
class EntityToElastic
{
    @Inject
    @Channel("entity-json-stream")
    Publisher<String> entities;
    
    @Inject
    RestHighLevelClient restHighLevelClient; 

    @ConsumeEvent("entity-json-stream")
    public void index(String strEntityAsJson) throws IOException 
    {
        IndexRequest request = new IndexRequest("track-entities"); 
        request.source(strEntityAsJson, XContentType.JSON); 
        restHighLevelClient.index(request, RequestOptions.DEFAULT); 
    }


}