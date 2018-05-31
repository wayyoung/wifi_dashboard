package wd.cs.wifi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;
import java.util.TreeMap;

public class ESQuery {
    public TreeMap<String,Map>  queryDevices(int type)throws Exception{
        TreeMap<String,Map> res=new TreeMap<String,Map>();
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
        try{
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

            SearchRequest searchRequest = new SearchRequest(ELK.INDEX_NAME);
            searchRequest.types("kamino_device");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(10);
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());

            searchRequest.scroll(scroll);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest);
            String scrollId=searchResponse.getScrollId();
            SearchHit[] searchHits=searchResponse.getHits().getHits();
            int x=0;
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {

                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    String mac=(String)sourceAsMap.get(KaminoDevice.KEY_MAC);
                    String hmac=(String)sourceAsMap.get(KaminoDevice.KEY_HMAC);
                    sourceAsMap.put("_id",hit.getId());
//                    System.out.println(mac+","+hmac);
                    if(type==0) {
                        if (mac != null) res.put(mac, sourceAsMap);
                        if (hmac != null) res.put(hmac, sourceAsMap);
                    }else if(type==1){
                        res.put((String)sourceAsMap.get("_id"), sourceAsMap);

                    }
                    x++;
                    // do something with the SearchHit
                }
                searchResponse = client.searchScroll(new SearchScrollRequest(scrollId).scroll(scroll));
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
//
//            SearchHit[] searchHits = hits.getHits();
//            for (SearchHit hit : searchHits) {
//                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//                String mac=(String)sourceAsMap.get(KaminoDevice.KEY_MAC);
//                String hmac=(String)sourceAsMap.get(KaminoDevice.KEY_HMAC);
//
//                System.out.println(mac+","+hmac);
//                // do something with the SearchHit
//            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
            boolean succeeded = clearScrollResponse.isSucceeded();
        }finally{
            try{client.close();}catch (Exception ex){}
        }

        return res;
    }


    public TreeMap<String,String>  queryDeviceHashedMacAgainstId()throws Exception{
        TreeMap<String,String> res=new TreeMap<String,String>();
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
        try{
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

            SearchRequest searchRequest = new SearchRequest(ELK.INDEX_NAME);
            searchRequest.types("kamino_device");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(10);
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());

            searchRequest.scroll(scroll);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest);
            String scrollId=searchResponse.getScrollId();
            SearchHit[] searchHits=searchResponse.getHits().getHits();
            int x=0;
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {

                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    String hmac=(String)sourceAsMap.get(KaminoDevice.KEY_HMAC);
                    res.put(hmac, hit.getId());
                    x++;
                    // do something with the SearchHit
                }
                searchResponse = client.searchScroll(new SearchScrollRequest(scrollId).scroll(scroll));
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
//
//            SearchHit[] searchHits = hits.getHits();
//            for (SearchHit hit : searchHits) {
//                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//                String mac=(String)sourceAsMap.get(KaminoDevice.KEY_MAC);
//                String hmac=(String)sourceAsMap.get(KaminoDevice.KEY_HMAC);
//
//                System.out.println(mac+","+hmac);
//                // do something with the SearchHit
//            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
            boolean succeeded = clearScrollResponse.isSucceeded();
        }finally{
            try{client.close();}catch (Exception ex){}
        }

        return res;
    }

    public TreeMap<String,Map> queryDevices()throws Exception{
        return this.queryDevices(0);
    }

    public static void main(String[] args)throws Exception{
        ESQuery eq=new ESQuery();
        eq.queryDevices();
    }

}
