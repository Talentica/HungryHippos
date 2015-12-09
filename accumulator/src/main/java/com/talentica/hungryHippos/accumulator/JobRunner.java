package com.talentica.hungryHippos.accumulator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class JobRunner {
    List<Job> jobs= new LinkedList<>();
    private Map<String,Map<Object, Node>> keyValueNodeNumberMap ;
    private DataDescription dataDescription;
   // private Map<String,Map<Object, Node>> keyValueNodeMapFileName;
    private DataStore dataStore;


    public JobRunner(DataDescription dataDescription, DataStore dataStore, Map<String,Map<Object, Node>> keyValueNodeNumberMap) {
        this.dataDescription = dataDescription;
        this.dataStore = dataStore;
        this.keyValueNodeNumberMap = keyValueNodeNumberMap;
    }

    private Map<Integer,List<Job>> primaryDimJobsMap = new HashMap<>();


    public void addJob(Job job){
        jobs.add(job);
        Integer primDim = job.getPrimaryDimension();
        List<Job> primDimList = primaryDimJobsMap.get(primDim);
        if(primDimList == null){
            primDimList = new LinkedList<>();
            primaryDimJobsMap.put(primDim,primDimList);
        }
        primDimList.add(job);
    }

    public void run(){
       /* try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+keyValueNodeNumberMap))){*/
           // keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) in.readObject();
            System.out.println(keyValueNodeNumberMap);

            DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
            List<RowProcessor> rowProcessors = new LinkedList<>();
            for(Integer primDim: primaryDimJobsMap.keySet()){
                StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
                for(Job job:primaryDimJobsMap.get(primDim)) {
                    RowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal,job);
                    storeAccess.addRowProcessor(rowProcessor);
                    rowProcessors.add(rowProcessor);
                }
                storeAccess.processRows();
            }

            rowProcessors.forEach(RowProcessor::finishUp);


        /*} catch (Exception e) {
            e.printStackTrace();
        }*/
    }

}
