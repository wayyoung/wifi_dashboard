package wd.cs.wifi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class Import0 {
    public static void main(String[] args) throws Exception {
        Import0 im=new Import0();
        try{im.createDatabase();}catch(Exception ex){}

        im.doImportFromConfluenceProd();
        im.doImportFromSUMO("e:/dev1_devices.csv","dev1");
        im.doImportFromSUMO("e:/qa1_devices.csv","qa1");
        im.doImportFromSUMO("e:/prod_devices.csv","prod");
    }

    public  void createDatabase()throws Exception{
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));

        CreateIndexRequest req=new CreateIndexRequest(ELK.INDEX_NAME);
//        CreateIndexRequest req1=new CreateIndexRequest("twplatform_dev1");
//        CreateIndexRequest req2=new CreateIndexRequest("twplatform_qa1");
        CreateIndexResponse res=client.indices().create(req);
//        CreateIndexResponse res2=client.indices().create(req2);



        client.close();
    }

    public void doImportFromSUMO(String fileName, String env) throws Exception {
        Pattern p = Pattern.compile("[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}");
        BufferedReader bfr = new BufferedReader(new FileReader(fileName));
        String line = null;

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
        try {
            ESQuery eq = new ESQuery();
            TreeMap<String, Map> devices = eq.queryDevices();
            System.out.println("DZ:"+devices.size());
            while ((line = bfr.readLine()) != null) {
                if (line.startsWith(",") || line.startsWith("\"_sourcename")) continue;
                String[] tks = line.split(",");
                String email = null;
                String mac = null;
                String hmac = null;

                Matcher m = p.matcher(tks[0].toLowerCase());
                if (m.find()) {
                    mac = m.group();
                    hmac = MAC.calculateHashed(mac);
                } else {
                    hmac = tks[0].substring(1, tks[0].length() - 1).toLowerCase();
                }
                if ((mac!=null&&devices.containsKey(mac)) || (hmac!=null && devices.containsKey(hmac))) {

                    System.out.println("existed device: mac=" + (mac==null?"":mac) + ", hashed_mac=" +  (hmac==null?"":hmac));
                    continue;
                }
                XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .field(KaminoDevice.KEY_MAC, mac)
                        .field(KaminoDevice.KEY_HMAC, hmac)
                        .field(KaminoDevice.KEY_ENV, env)
                        .field(KaminoDevice.KEY_MONITOR, 1)
                        .field(KaminoDevice.KEY_USEREMAIL, email)
                        .field(KaminoDevice.KEY_CATEGORY, KaminoDevice.CATEGORY_YODA)
                        .endObject();
                IndexResponse response = client.index(new IndexRequest(ELK.INDEX_NAME, "kamino_device").source(builder));


//            System.out.println(email+", "+)

            }
        } finally {
            client.close();
        }
    }

    public void doImportFromConfluenceProd() throws Exception {
        Pattern p = Pattern.compile("[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}");
        BufferedReader bfr = new BufferedReader(new FileReader("D:/prod.csv"));
        String line = null;

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                        new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
        try {
            ESQuery eq = new ESQuery();
            TreeMap<String, Map> devices = eq.queryDevices();
            int count=0;
            while ((line = bfr.readLine()) != null) {
                System.out.println("count: "+(count++));
                if (line.startsWith(",")) continue;
                String[] tks = line.split(",");
                String email = (tks[1].trim().length() > 0) ? tks[1].trim() : null;
                Matcher m = p.matcher(tks[2].toLowerCase());

                String mac = null;
                String hmac = null;
                if (m.find()) {
                    mac = m.group();
                    hmac = MAC.calculateHashed(mac);
                }
                if (devices.containsKey(mac) || devices.containsKey(hmac)) {

                    System.out.println("existed device: mac=" + mac + ", hashed_mac=" + MAC.calculateHashed(mac));
                    continue;
                }
                XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .field(KaminoDevice.KEY_MAC, mac)
                        .field(KaminoDevice.KEY_HMAC, hmac)
                        .field(KaminoDevice.KEY_ENV, "prod")
                        .field(KaminoDevice.KEY_MONITOR, 1)
                        .field(KaminoDevice.KEY_USEREMAIL, email)
                        .field(KaminoDevice.KEY_CATEGORY, KaminoDevice.CATEGORY_YODA)
                        .endObject();
                IndexResponse response = client.index(new IndexRequest(ELK.INDEX_NAME, "kamino_device").source(builder));




            }
        } finally {
            client.close();
        }
    }

}
