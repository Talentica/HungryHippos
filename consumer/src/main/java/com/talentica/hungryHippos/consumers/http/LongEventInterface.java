package com.talentica.hungryHippos.consumers.http;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.MaxSize;

/**
 * Created by abhinavn on 10/6/15.
 */
public interface LongEventInterface extends Byteable
{
    public void setOwner(@MaxSize(1024) String value);
    public String getOwner();
}
