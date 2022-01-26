package com.mboysan.consensus.message;

import java.io.Serializable;
import java.util.Objects;

public record LogEntry(String command, int term) implements Serializable {
}
