package com.mboysan.consensus;

import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.SimMessage;

import java.io.IOException;

public class SimClient extends AbstractClient {
    SimClient(Transport transport) {
        super(transport);
    }

    public SimMessage simulate(SimMessage message) throws IOException {
        return (SimMessage) getTransport().sendRecv(message);
    }

    public CustomResponse customRequest(CustomRequest request) throws IOException {
        return (CustomResponse) getTransport().sendRecv(request);
    }
}
