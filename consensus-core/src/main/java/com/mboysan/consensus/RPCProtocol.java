package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.util.Set;

interface RPCProtocol {
    void onNodeListChanged(Set<Integer> serverIds);
    Message processRequest(Message request) throws IOException;
}
