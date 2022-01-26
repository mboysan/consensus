package com.mboysan.consensus.message;

import java.io.Serializable;

public record LogEntry(String command, int term) implements Serializable {
}
