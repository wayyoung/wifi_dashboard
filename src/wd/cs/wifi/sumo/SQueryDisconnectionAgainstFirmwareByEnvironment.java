package wd.cs.wifi.sumo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sumologic.client.Credentials;
import com.sumologic.client.SumoLogicClient;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetRecordsForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetSearchJobStatusResponse;
import com.sumologic.client.searchjob.model.SearchJobRecord;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SQueryDisconnectionAgainstFirmwareByEnvironment {

    SimpleDateFormat isosdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    String start;
    String end;
    SumoLogicClient sumoClient;

    String searchJobId = null;
    ObjectMapper objectMapper = new ObjectMapper();


    String environment="prod";

    public SQueryDisconnectionAgainstFirmwareByEnvironment(){

        Credentials credential = new Credentials(SUMO.accessId, SUMO.accessKey);
        sumoClient = new SumoLogicClient(credential);
        try{sumoClient.setURL(SUMO.url);}catch(Exception ex){ex.printStackTrace();}
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

    }


    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }


    public void setStart(String start){
        this.start=start;

    }
    public void setEnd(String end){
        this.end=end;
    }

    public String getStart(){
        return this.start;

    }
    public String getEnd(){
        return this.end;
    }


    public synchronized TreeMap<String, TreeMap<String,Integer>> doQuery() throws Exception {
        TreeMap<String, TreeMap<String,Integer>> result = new TreeMap<String, TreeMap<String,Integer>>();
        String searchJobId = null;
        try {
//            String sn="";
//            if(sourcename!=null)sn+="_sourceName="+sourcename;
//            if(hashedSourcename!=null){
//                if(sn.length()>0)sn+=" or ";
//                sn+="_sourcename="+hashedSourcename;
//            }
//            if(sn.length()>0){
//                sn="("+sn+")";
//            }else{
//                throw new RuntimeException("_sourcename must be specified!!");
//            }
            String query =  "(_sourceCategory="+environment+"/device/*/restsdk and \"wifi disconnected\" and fw_version=5.1.0-*) | count by _sourcename,fw_version | sort by _sourcename,fw_version asc";
            System.out.println("S-E: " + start + "-" + end);
            System.out.println("query: " + query);
            searchJobId = sumoClient.createSearchJob(query, start, end, "UTC");
            GetSearchJobStatusResponse getSearchJobStatusResponse = null;
            int messageCount = 0;

            while (getSearchJobStatusResponse == null ||
                    (!getSearchJobStatusResponse.getState().equals("DONE GATHERING RESULTS") &&
                            !getSearchJobStatusResponse.getState().equals("CANCELLED"))) {

                // Sleep for a little bit, so we don't hammer
                // the Sumo Logic service.
                Thread.sleep(1000);

                // Get the latest search job status.
                getSearchJobStatusResponse = sumoClient.getSearchJobStatus(searchJobId);

                // Extract the message and record counts for
                // using them later down the road.
                messageCount = getSearchJobStatusResponse.getMessageCount();


                // Tell the user what's going on. Class
                // GetSearchJobStatusResponse has a nice toString()
                // implementation that will show the status and
                // the message and record counts.
                if (messageCount > 0) {
                    System.out.printf(
                            "Search job ID: '%s', %s\n",
                            searchJobId,
                            getSearchJobStatusResponse);
                }
            }

            //start to export the result
            int messageToExport=messageCount;
            GetMessagesForSearchJobResponse getMessagesForSearchJobResponse = sumoClient.getMessagesForSearchJob(searchJobId, 0, messageCount);
            GetRecordsForSearchJobResponse records=sumoClient.getRecordsForSearchJob(searchJobId, 0, messageCount);

            try {

                List<SearchJobRecord> messages = records.getRecords();
                for (SearchJobRecord message : messages) {
                    Map<String, String> fields = message.getMap();
//                    List<String> fieldNames = new ArrayList<String>(message.getFieldNames());
//                    Collections.sort(fieldNames);
                    String sn=fields.get("_sourcename");
                    TreeMap<String,Integer> fw_recordMap=result.get(sn);
                    if(fw_recordMap==null){
                        fw_recordMap=new TreeMap<>();
                    }
                    System.out.println("DISC_RECORD:"+sn+","+fields.get("fw_version")+","+fields.get("_count"));
                    fw_recordMap.put(fields.get("fw_version"),Integer.parseInt(fields.get("_count")));
                    result.put(sn,fw_recordMap);
                }
                System.out.println("done!!\n");

            } catch (Exception ex) {
                ex.printStackTrace();
            }


        } catch (Throwable t) {

            // Yikes. We has an error.
            t.printStackTrace();

        } finally {

            try {
                sumoClient.cancelSearchJob(searchJobId);

            } catch (Throwable t) {
                //System.out.printf("Error cancelling search job: '%s'", t.getMessage());
                //t.printStackTrace();
            }
        }


        return result;
    }

    public static void main(String[] args)throws Exception{
        SQueryDisconnectionAgainstFirmwareByEnvironment qa=new SQueryDisconnectionAgainstFirmwareByEnvironment();

        qa.setStart("2018-04-03T00:00:00Z");
        qa.setEnd("2018-05-31T00:00:00Z");
        //qa.queryFirwareVersionBegin();
        //qa.queryDisconnectionCountOverFirmwareVersion();
        qa.setEnvironment("prod");
        qa.doQuery();
        qa.setEnvironment("dev1");
        qa.doQuery();
        qa.setEnvironment("qa1");
        qa.doQuery();

    }


}
