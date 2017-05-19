package com.talentica.hungryHippos.storage.util;

/**
 * Created by rajkishoreh on 18/5/17.
 */
public class Counter {
    int count;

    public Counter(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void reset(){
        this.count = 0;
    }

    public void increment(int number){
        this.count+=number;
    }

    public String toString(){
        return count+"";
    }
}
