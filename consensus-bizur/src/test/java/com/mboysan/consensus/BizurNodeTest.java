package com.mboysan.consensus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BizurNodeTest extends BizurTestBase {

    @Test
    void testLeaderElected() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        // wait a while and check again to see if leader remained unchanged
        advanceTimeForElections();
        assertEquals(leaderId, assertOneLeader());
    }

}
