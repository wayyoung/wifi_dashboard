package wd.cs.wifi;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import wd.cs.wifi.sumo.SQueryDisconnectionAgainstFirmwareByEnvironment;
import wd.cs.wifi.sumo.SQueryFirmwareHistoryByEnvironment;

import java.text.SimpleDateFormat;
import java.util.*;

public class SyncAgent {

    String start;
    String end;
    String environment="prod";

    SimpleDateFormat isosdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    ESQuery eq=new ESQuery();

    public void updateDisconnectionCountOnBuild(){
        final RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));

        try{

            SQueryDisconnectionAgainstFirmwareByEnvironment sqde=new SQueryDisconnectionAgainstFirmwareByEnvironment();
            sqde.setEnvironment(this.environment);
            sqde.setStart(this.start);
            sqde.setEnd(this.end);

            TreeMap<String, TreeMap<String,Integer>> sqdeMap=sqde.doQuery();

            TreeMap<String, Object> deviceMap = eq.queryDevices(1);
            deviceMap.keySet().iterator().forEachRemaining(docId -> {
                if(!(((Map) deviceMap.get(docId)).get(KaminoDevice.KEY_ENV)).equals(this.environment))return;
                try {
                    TreeMap<String, Integer> disc_on_buildMap = new TreeMap<String, Integer>();
                    String mac = (String) ((Map) deviceMap.get(docId)).get(KaminoDevice.KEY_MAC);
                    String hmac = (String) ((Map) deviceMap.get(docId)).get(KaminoDevice.KEY_HMAC);


                    //
                    ArrayList old = (ArrayList) (((Map) deviceMap.get(docId)).get("wifi_disconnect_count_on_build"));
                    if (old != null) {
                        old.iterator().forEachRemaining(v -> {
                            Map fw_record = ((Map) v);
                            disc_on_buildMap.put((String) ((Map) v).get("fw_version"), (Integer) ((Map) v).get("count"));
                        });
                    }
                    if (mac != null && sqdeMap.containsKey(mac)) {

                        sqdeMap.get(mac).keySet().iterator().forEachRemaining(fw_version -> {
                            disc_on_buildMap.put(fw_version, (Integer) sqdeMap.get(mac).get(fw_version));
                        });
                    }
                    if (sqdeMap.containsKey(hmac)) {
                        sqdeMap.get(hmac).keySet().iterator().forEachRemaining(fw_version -> {
                            disc_on_buildMap.put(fw_version, (Integer) sqdeMap.get(hmac).get(fw_version));
                        });
                    }
                    XContentBuilder xb = XContentFactory.jsonBuilder().startObject();
                    xb.startArray("wifi_disconnect_count_on_build");
                    Iterator itr = disc_on_buildMap.keySet().iterator();
                    while (itr.hasNext()) {
                        String fw_version = (String) itr.next();
                        xb.startObject().field("fw_version", fw_version).field("count", disc_on_buildMap.get(fw_version));
                        xb.endObject();
                    }

                    xb.endArray().endObject();
                    System.out.println(docId + "," + xb.string());

                    UpdateRequest updateRequest = new UpdateRequest(ELK.INDEX_NAME, "kamino_device", docId).doc(xb);
                    DocWriteResponse.Result res = client.update(updateRequest).getResult();
                    System.out.println("docid:" + docId + "," + res.toString());
                }catch (Exception exx){
                    exx.printStackTrace();
                }
            });

        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            try{client.close();}catch(Exception ex){}
        }
    }

    public void updateFirmwareHistory(){
        final RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
        try{


            SQueryFirmwareHistoryByEnvironment sqfe=new SQueryFirmwareHistoryByEnvironment();

            sqfe.setEnvironment(this.environment);
            sqfe.setStart(this.start);
            sqfe.setEnd(this.end);

            TreeMap<String, TreeMap<String,Long>> sqfeMap=sqfe.doQuery();

            TreeMap<String, Object> deviceMap = eq.queryDevices(1);
//            TreeMap<String, String> hmacIdMap = eq.queryDeviceHashedMacAgainstId();

            deviceMap.keySet().iterator().forEachRemaining(docId -> {
                if(!(((Map) deviceMap.get(docId)).get(KaminoDevice.KEY_ENV)).equals(this.environment))return;
                TreeMap<String, Long[]> fw_historyMap = new TreeMap<String, Long[]>();

                String mac = (String) ((Map)deviceMap.get(docId)).get(KaminoDevice.KEY_MAC);
                String hmac = (String)((Map)deviceMap.get(docId)).get(KaminoDevice.KEY_HMAC);
                try {

                    ArrayList old=(ArrayList)(((Map)deviceMap.get(docId)).get("fw_history"));
                    if(old!=null){
                        old.iterator().forEachRemaining(v->{
                            Map fw_record=((Map)v);
                            Long[] se={(Long)fw_record.get("start"),(Long)fw_record.get("end")};
                            fw_historyMap.put((String)((Map)v).get("fw_version"),se);
                        });
                    }


                    if (mac != null && sqfeMap.containsKey(mac)) {

                        sqfeMap.get(mac).keySet().iterator().forEachRemaining(fw_version -> {
//                            System.out.println("D="+sqfeMap.get(mac).get(fw_version));
                            Long[] se = {sqfeMap.get(mac).get(fw_version), 0L};
                            fw_historyMap.put(fw_version, se);
                        });
                    }
                    if ( sqfeMap.containsKey(hmac)) {
                        sqfeMap.get(hmac).keySet().iterator().forEachRemaining(fw_version -> {
                            Long[] se = {sqfeMap.get(hmac).get(fw_version), 0L};
                            fw_historyMap.put(fw_version, se);
                        });
                    }

                    Long[] previous_se = {0L,0L};//new Date[2];
                    Iterator itr = fw_historyMap.keySet().iterator();
                    while (itr.hasNext()) {
                        String fw_version = (String) itr.next();
//                            String[] se={isosdf.format( qfeMap.get(mac).get(fw_version)),isosdf.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime())};

                        Long[] se = fw_historyMap.get(fw_version);
                        if (previous_se != null) {
                            previous_se[1] = se[0];
                        }
                        previous_se = se;
                    }

                    XContentBuilder xb =  XContentFactory.jsonBuilder().startObject();
                    xb.startArray("fw_history");
                    itr = fw_historyMap.keySet().iterator();
                    while (itr.hasNext()) {
                        String fw_version = (String) itr.next();
                        Long[] se = fw_historyMap.get(fw_version);
                        xb.startObject().field("fw_version",fw_version).field("start",se[0]);
                        if(se[1]!=null){
                            xb.field("end",se[1]);
                        }
                        xb.endObject();
                    }

                    xb.endArray().endObject();

                    System.out.println(docId+","+xb.string());
                    UpdateRequest updateRequest = new UpdateRequest(ELK.INDEX_NAME, "kamino_device", docId).doc(xb);
                    DocWriteResponse.Result res = client.update(updateRequest).getResult();


                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            });


        }catch(Exception ex){
            ex.printStackTrace();
        }finally {
            try{client.close();}catch(Exception ex){}
        }

    }



    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public static void main(String[] args)throws Exception{
        SyncAgent sqa=new SyncAgent();
//        qa.init();
        sqa.setStart("2018-04-10T00:00:00Z");
        sqa.setEnd("2018-06-01T00:00:00Z");

        sqa.setEnvironment("dev1");
        sqa.updateFirmwareHistory();
        sqa.updateDisconnectionCountOnBuild();

        sqa.setEnvironment("qa1");
        sqa.updateFirmwareHistory();
        sqa.updateDisconnectionCountOnBuild();

        sqa.setEnvironment("prod");
        sqa.updateFirmwareHistory();
        sqa.updateDisconnectionCountOnBuild();

    }
}
