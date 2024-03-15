package com.mboysan.consensus.message;

public class CustomRequest extends RoutableRequest {

    public interface Command {
        String PING = "ping";
    }

    private final String command;
    private final String arguments;

    public CustomRequest(String command) {
        this(command, null);
    }

    public CustomRequest(String command, String arguments) {
        this(ROUTE_TO_SELF, command, arguments);
    }

    public CustomRequest(int routeTo, String command, String arguments) {
        super(routeTo);
        this.command = command;
        this.arguments = arguments;
    }

    public String getCommand() {
        return command;
    }

    public String getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "CustomRequest{" +
                "command='" + command + '\'' +
                ", arguments='" + arguments + '\'' +
                "} " + super.toString();
    }
}
