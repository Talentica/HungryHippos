import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataReader {
    private static Map<String,Map<Object, Node>> keyValueNodeNumberMap ;

    public static void main(String [] args) throws IOException {
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE,0);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);

        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});

        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream("keyValueNodeNumberMap"))){
            keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) in.readObject();
            System.out.println(keyValueNodeNumberMap);
        } catch (Exception e) {
            e.printStackTrace();
        }



        byte[] buf = new byte[dataDescription.getSize()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);


        NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                = new NodeDataStoreIdCalculator(keyValueNodeNumberMap,1,dataDescription);
        FileDataStore dataStore = new FileDataStore(3, nodeDataStoreIdCalculator, dataDescription, true);
        StoreAccess storeAccess = dataStore.getStoreAccess(0);
        storeAccess.addRowProcessor(new TestRowProcessor(dataDescription, dynamicMarshal));
        storeAccess.processRows();

    }
}