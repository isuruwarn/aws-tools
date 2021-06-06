package org.warn.aws.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.Getter;
import org.slf4j.LoggerFactory;

public class LoggingUtils {

    @Getter
    private static Logger fileOutputLogger;

    @Getter
    private static Logger consoleLogger;

    static {
        System.setProperty( ConfigConstants.AWSTOOLS_LOG_PROPERTY_NAME, ConfigConstants.AWSTOOLS_LOG_FILE );

        fileOutputLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger( ConfigConstants.LOGBACK_FILE_OUTPUT_LOGGER );
        //fileOutputLogger.setLevel(Level.INFO);

        consoleLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger( ConfigConstants.LOGBACK_CONSOLE_OUTPUT_LOGGER );
        //consoleLogger.setLevel(Level.INFO);
    }

}
