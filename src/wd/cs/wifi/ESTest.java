package wd.cs.wifi;

import javafx.scene.NodeBuilder;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.node.Node;
//import org.elasticsearch.node.No

public class ESTest {
    public static void main(String[] args)throws Exception{
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));

        //CreateIndexRequest req=new CreateIndexRequest(ELK.INDEX_NAME);
//        CreateIndexRequest req1=new CreateIndexRequest("twplatform_dev1");
//        CreateIndexRequest req2=new CreateIndexRequest("twplatform_qa1");
       // CreateIndexResponse res=client.indices().create(req);
//        CreateIndexResponse res2=client.indices().create(req2);



        client.close();
    }
}
