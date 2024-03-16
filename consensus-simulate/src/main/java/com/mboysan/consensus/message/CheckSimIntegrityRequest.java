package com.mboysan.consensus.message;

public class CheckSimIntegrityRequest extends RoutableRequest {

    public CheckSimIntegrityRequest(int routeTo) {
        super(routeTo);
    }

    @Override
    public String toString() {
        return "CheckSimIntegrityRequest{} " + super.toString();
    }
}
