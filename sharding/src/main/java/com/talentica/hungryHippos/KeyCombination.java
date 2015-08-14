package com.talentica.hungryHippos;

import java.util.Map;

/**
 * Created by debasishc on 14/8/15.
 */
public class KeyCombination {
    private Map<String,Object> keyValueCombination;

    public KeyCombination(Map<String, Object> keyValueCombination) {
        this.keyValueCombination = keyValueCombination;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyCombination that = (KeyCombination) o;

        return !(keyValueCombination != null ? !keyValueCombination.equals(that.keyValueCombination) :
                that.keyValueCombination != null);

    }

    @Override
    public int hashCode() {
        return keyValueCombination != null ? keyValueCombination.hashCode() : 0;
    }

    public Map<String, Object> getKeyValueCombination() {

        return keyValueCombination;
    }

    public void setKeyValueCombination(Map<String, Object> keyValueCombination) {
        this.keyValueCombination = keyValueCombination;
    }
}
