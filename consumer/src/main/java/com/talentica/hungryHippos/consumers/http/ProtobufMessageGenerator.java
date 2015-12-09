package com.talentica.hungryHippos.consumers.http;

import static com.talentica.hungryHippos.msg.Message.MessageDemo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Created by debasishc on 15/6/15.
 */
public class ProtobufMessageGenerator {
    public static void main(String [] args) throws IOException {
        MessageDemo messageDemo = MessageDemo.newBuilder().setName("A name").build();

        FileOutputStream fout = new FileOutputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"tempData");
        messageDemo.writeTo(fout);
        fout.flush();
        fout.close();
    }
}
