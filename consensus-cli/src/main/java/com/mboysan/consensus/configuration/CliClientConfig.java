package com.mboysan.consensus.configuration;

public interface CliClientConfig extends CoreConfig {

        interface Param {
            String INTERACTIVE = "interactive";
            String COMMAND = "command";
            String ARGUMENTS = "arguments";
            String ROUTE_TO = "routeTo";
        }

        @Key(Param.INTERACTIVE)
        @DefaultValue("true")
        boolean interactive();

        @Key(Param.COMMAND)
        String command();

        @Key(Param.ARGUMENTS)
        String arguments();

        @Key(Param.ROUTE_TO)
        @DefaultValue("-1")
        int routeTo();
}
