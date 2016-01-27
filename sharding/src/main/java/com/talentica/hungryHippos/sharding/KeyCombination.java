package com.talentica.hungryHippos.sharding;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by debasishc on 14/8/15.
 */
public class KeyCombination implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3581984005135868712L;
	private Map<String,Object> keyValueCombination;

    public KeyCombination(Map<String, Object> keyValueCombination) {
        this.keyValueCombination = keyValueCombination;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
		if (o == null || !(o instanceof KeyCombination)) {
			return false;
		}
        KeyCombination that = (KeyCombination) o;
        return keyValueCombination.equals(that.keyValueCombination);

    }

    public boolean checkMatchAnd(KeyCombination rhs){
        for(String k:rhs.keyValueCombination.keySet()){
            Object thatValue = rhs.keyValueCombination.get(k);
            if(!keyValueCombination.containsKey(k)) {
                continue;
            }else{
                Object thisValue = keyValueCombination.get(k);
                if(thisValue==null){
                    continue;
                }else{
                    if(thisValue.equals(thatValue)){
                        continue;
                    }else{
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public boolean checkMatchOr(KeyCombination rhs){
        for(String k:rhs.keyValueCombination.keySet()){
            Object thatValue = rhs.keyValueCombination.get(k);
            if(!keyValueCombination.containsKey(k)) {
                continue;
            }else{
                Object thisValue = keyValueCombination.get(k);
                if(thisValue==null){
                    continue;
                }else{
                    if(thisValue.equals(thatValue)){
                        return true;
                    }else{
                        continue;
                    }
                }
            }
        }
        return false;
    }

    public boolean checkMatchOr(List<KeyCombination> rhs){
        for(KeyCombination k:rhs){
            if(this.checkMatchOr(k)){
                return true;
            }
        }
        return false;
    }


    @Override
    public int hashCode() {
        return keyValueCombination != null ? keyValueCombination.toString().hashCode() : 0;
    }

    public Map<String, Object> getKeyValueCombination() {

        return keyValueCombination;
    }

    public void setKeyValueCombination(Map<String, Object> keyValueCombination) {
        this.keyValueCombination = keyValueCombination;
    }

    @Override
    public String toString() {
        return "KeyCombination{" +
                "keyValueCombination=" + keyValueCombination +
                '}';
    }
}
