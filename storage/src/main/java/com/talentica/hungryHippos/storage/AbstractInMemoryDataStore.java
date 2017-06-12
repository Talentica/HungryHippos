package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.storage.util.Counter;

import java.io.IOException;
import java.util.*;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public abstract class AbstractInMemoryDataStore implements DataStore{

    private Map<String, List<byte[]>> map;
    private Map<String, byte[]> latestByteMap;
    private int byteArraySize;
    private String hhFilePath;
    private int batchSize = 5;
    private Map<String, Counter> latestByteArraySize;
    private byte[] byteArray;
    private int maxKeys = 500000;
    private int countKeys;
    private int byteArrayBatchSize;

    public AbstractInMemoryDataStore(String hhFilePath, int byteArraySize, int noOfFiles) {
        this.byteArraySize = byteArraySize;
        this.hhFilePath = hhFilePath;
        maxKeys = maxKeys<noOfFiles?maxKeys:noOfFiles;
        this.map = new HashMap<>(maxKeys, 1);
        this.latestByteArraySize = new HashMap<>(maxKeys, 1);
        this.latestByteMap = new HashMap<>(maxKeys,1);
        this.countKeys = 0;

        if(maxKeys>300000){
            batchSize =1;
        }else if(maxKeys>100000){
            batchSize = 2;
        }
        byteArrayBatchSize = batchSize * byteArraySize;
    }

    public void reset() {
        this.map.clear();
        this.latestByteArraySize.clear();
        this.countKeys = 0;
    }

    @Override
    public boolean storeRow(String key, byte[] arr) throws IOException {
        List<byte[]> byteList = map.get(key);
        byteArray = latestByteMap.get(key);
        if (byteArray == null && countKeys >= maxKeys) {
            return handleOnMaxKeysCount(key, arr);
        }
        if (byteArray == null) {
            byteList = new LinkedList<>();
            map.put(key, byteList);
            countKeys++;
            latestByteArraySize.put(key, new Counter(byteArrayBatchSize));
        }
        Counter counter = latestByteArraySize.get(key);
        if (counter.getCount() == byteArrayBatchSize) {
            byteArray = new byte[byteArrayBatchSize];
            byteList.add(byteArray);
            latestByteMap.put(key,byteArray);
            counter.reset();
        }
        System.arraycopy(arr, 0, byteArray, counter.getCount(), byteArraySize);
        counter.increment(byteArraySize);
        return true;
    }

    public Iterator<byte[]> getIterator(String id) {
        return map.get(id).iterator();
    }

    public long size(String key) {
        Counter counter = latestByteArraySize.get(key);
        if (counter.getCount() != byteArrayBatchSize) {
            return ((long) map.get(key).size() - 1) * byteArrayBatchSize +counter.getCount();
        }
        return ((long) map.get(key).size()) * byteArrayBatchSize;
    }

    public boolean containsKey(String key){
        return map.containsKey(key);
    }

    public int getByteArraySize() {
        return byteArraySize;
    }

    public int getLatestByteArraySize(String key) {
        return latestByteArraySize.get(key).getCount();
    }

    public int getByteArrayCount(String key) {
        return map.get(key).size();
    }

    public int getByteArrayBatchSize() {
        return byteArrayBatchSize;
    }


    @Override
    public void storeRow(int index, byte[] raw) {

    }

    @Override
    public void sync() {

    }

    @Override
    public String getHungryHippoFilePath() {
        return hhFilePath;
    }

    public abstract boolean handleOnMaxKeysCount(String key, byte[] arr) throws IOException;

    public int getMaxKeys() {
        return maxKeys;
    }
}
