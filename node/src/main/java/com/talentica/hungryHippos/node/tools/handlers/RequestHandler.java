package com.talentica.hungryHippos.node.tools.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * This is an interface which must be implemented by the RequestHandler classes
 * Created by rajkishoreh on 18/8/16.
 */
public interface RequestHandler {

    void handleRequest(DataInputStream dis, DataOutputStream dos) throws IOException;
}
