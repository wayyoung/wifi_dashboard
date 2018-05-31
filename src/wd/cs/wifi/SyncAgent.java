package wd.cs.wifi;

import org.apache.http.HttpHost;
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
    RestHighLevelClient client = null;
    String start;
    String end;
    String environment="prod";

    SimpleDateFormat isosdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public void doWork(){
        try{
            client = new RestHighLevelClient(RestClient.builder(
                            new HttpHost(ELK.SERVER_IP, ELK.ES_PORT, "http"),
                            new HttpHost(ELK.SERVER_IP, ELK.ES_PORT2, "http")));
            ESQuery eq=new ESQuery();
            SQueryFirmwareHistoryByEnvironment sqfe=new SQueryFirmwareHistoryByEnvironment();
            SQueryDisconnectionAgainstFirmwareByEnvironment sqde=new SQueryDisconnectionAgainstFirmwareByEnvironment();
            sqfe.setEnvironment(this.environment);
            sqfe.setStart(this.start);
            sqfe.setEnd(this.end);
            sqde.setEnvironment(this.environment);
            sqde.setStart(this.start);
            sqde.setEnd(this.end);

            TreeMap<String, TreeMap<String,String>> sqfeMap=sqfe.doQuery();
            TreeMap<String, TreeMap<String,Integer>> sqdeMap=sqde.doQuery();



            TreeMap<String, Map> deviceMap = eq.queryDevices(1);
//            TreeMap<String, String> hmacIdMap = eq.queryDeviceHashedMacAgainstId();

            deviceMap.keySet().iterator().forEachRemaining(docId -> {
//                String docId=hmacIdMap.get(hmac);
                TreeMap<String, String[]> fw_historyMap = new TreeMap<String, String[]>();
                String mac = (String) deviceMap.get(docId).get(KaminoDevice.KEY_MAC);
                String hmac = (String) deviceMap.get(docId).get(KaminoDevice.KEY_HMAC);
                try {

                    if (mac != null && sqfeMap.containsKey(mac)) {

                        sqfeMap.get(mac).keySet().iterator().forEachRemaining(fw_version -> {
//                            System.out.println("D="+sqfeMap.get(mac).get(fw_version));
                            String[] se = {sqfeMap.get(mac).get(fw_version), null};
                            fw_historyMap.put(fw_version, se);
                        });
                    }
                    if ( sqfeMap.containsKey(hmac)) {
                        sqfeMap.get(hmac).keySet().iterator().forEachRemaining(fw_version -> {
                            String[] se = {sqfeMap.get(hmac).get(fw_version), null};
                            fw_historyMap.put(fw_version, se);
                        });
                    }

                    String[] previous_se = null;//new Date[2];
                    Iterator itr = fw_historyMap.keySet().iterator();
                    while (itr.hasNext()) {
                        String fw_version = (String) itr.next();
//                            String[] se={isosdf.format( qfeMap.get(mac).get(fw_version)),isosdf.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime())};

                        String[] se = fw_historyMap.get(fw_version);
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
                        String[] se = fw_historyMap.get(fw_version);
                        xb.startObject().field("fw_version",fw_version).field("start",se[0]);
                        if(se[1]!=null){
                            xb.field("end",se[1]);
                        }
                        xb.endObject();
                    }

                    xb.endArray().endObject();

                    System.out.println(docId+","+xb.string());

                    UpdateRequest updateRequest = new UpdateRequest(ELK.INDEX_NAME,"kamino_device",docId).doc(xb);
                    System.out.println("docid:"+docId+","+client.update(updateRequest).getResult().toString());

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            });

            //prepared




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
        SyncAgent qa=new SyncAgent();
//        qa.init();
        qa.setStart("2018-05-01T00:00:00Z");
        qa.setEnd("2018-05-30T00:00:00Z");
//        qa.setSourcename("00:14:ee:0c:6c:df");
//        qa.setHashedSourcename(MAC.calculateHashed(qa.getSourcename()));
//        qa.queryFirwareVersionBegin();
//        qa.queryDisconnectionCountOverFirmwareVersion();

        qa.doWork();
    }
}
