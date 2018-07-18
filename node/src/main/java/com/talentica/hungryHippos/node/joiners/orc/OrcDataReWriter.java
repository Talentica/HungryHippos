/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.node.joiners.orc;

import org.apache.hadoop.fs.Path;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

public class OrcDataReWriter implements Callable<Boolean> {

    private volatile boolean alive = true;

    private Queue<OrcStoreIncrementalDataEntity> mergeDataEntities;

    public OrcDataReWriter(Queue<OrcStoreIncrementalDataEntity> mergeDataEntities) {
        this.mergeDataEntities = mergeDataEntities;
    }

    @Override
    public Boolean call() throws Exception {
        OrcStoreIncrementalDataEntity dataEntity;
        List<Path> pathList = new LinkedList<>();
        while (alive){
            dataEntity = mergeDataEntities.poll();
            if(dataEntity!=null){
                pathList.clear();
                for(String srcPath:dataEntity.getSrcPaths()){
                    pathList.add(new Path(srcPath));
                }
                OrcFileMerger.rewriteData(new Path(dataEntity.getDestPath()),pathList,true,false);
            }else {
                Thread.sleep(1000);
            }
        }
        dataEntity = mergeDataEntities.poll();
        while (dataEntity!=null){
            pathList.clear();
            for(String srcPath:dataEntity.getSrcPaths()){
                pathList.add(new Path(srcPath));
            }
            OrcFileMerger.rewriteData(new Path(dataEntity.getDestPath()),pathList,true,false);
            dataEntity = mergeDataEntities.poll();
        }

        return true;
    }

    public void kill(){
        alive = false;
    }

    public void addDataEntities(OrcStoreIncrementalDataEntity mergeDataEntity) {
        while(!this.mergeDataEntities.offer(mergeDataEntity));
    }
}
