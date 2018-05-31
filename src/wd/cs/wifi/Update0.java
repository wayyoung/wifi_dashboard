package wd.cs.wifi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Update0 {
    public static void main(String[] args)throws Exception{
        Pattern p=Pattern.compile("[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}");
//        BufferedReader bfr=new BufferedReader(new FileReader("D:/prod.csv"));
        BufferedReader bfr=new BufferedReader(new FileReader("E:/dev1_devices.csv"));
        String line=null;

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
try {
    ESQuery eq=new ESQuery();
    TreeMap<String,Map> devices=eq.queryDevices();
    System.out.println(devices.size());
    while ((line = bfr.readLine()) != null) {
        if (line.startsWith(",")) continue;
        String[] tks = line.split(",");
//        String email = (tks[1].trim().length() > 0) ? tks[1].trim() : null;
        Matcher m = p.matcher(tks[0].toLowerCase());
        String email=null;
        String mac = null;
        String hmac = null;
        if (m.find()) {
            mac = m.group();
            hmac = MAC.calculateHashed(mac);
        }else{
            hmac=tks[0].substring(1,tks[0].length()-1);
        }
//        if((mac!=null && devices.containsKey(mac)) || (hmac!=null && devices.containsKey(hmac))) {
//
//            if(mac!=null)
//                System.out.println(devices.get(mac).get("_id"));
//
//
//            //System.out.println("!!!!existed device: mac=" + mac + ", hashed_mac=" + MAC.calculateHashed(mac));
//            continue;
//        }
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field(KaminoDevice.KEY_MAC, mac)
                .field(KaminoDevice.KEY_HMAC, hmac)
                .field(KaminoDevice.KEY_ENV, "prod")
                .field(KaminoDevice.KEY_MONITOR, 1)
                .field(KaminoDevice.KEY_USEREMAIL, email)
                .field(KaminoDevice.KEY_CATEGORY, KaminoDevice.CATEGORY_YODA)
                .endObject();

        if((mac!=null && devices.containsKey(mac)) || (hmac!=null && devices.containsKey(hmac))) {

            String id=null;
            if(mac!=null)
                id=(String)devices.get(mac).get("_id");
            else
                id=(String)devices.get(hmac).get("_id");

            UpdateRequest request = new UpdateRequest("twplatform1", "kamino_device", "1");

            //System.out.println("!!!!existed device: mac=" + mac + ", hashed_mac=" + MAC.calculateHashed(mac));
            continue;
        }
        IndexResponse response = client.index(new IndexRequest("twplatform1","kamino_device").source(builder));


//            System.out.println(email+", "+)

    }
}finally {
    client.close();
}
    }

}
