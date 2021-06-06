package org.warn.aws.util;

import org.warn.utils.core.Env;
import org.warn.utils.datetime.DateTimeUtil;

public class ConfigConstants {

	public static final String AWSTOOLS_HOME_DIR_NAME = ".awstools";
	public static final String AWSTOOLS_CONFIG_FILE = "config.json";
	public static final String AWSTOOLS_BACKUP_LOG_FILE = AWSTOOLS_HOME_DIR_NAME + Env.FILE_SEPERATOR + "backuplog.json";

	public static final String APP_PROPERTY_FILE_NAME = "application.properties";
	public static final String APP_VERSION = "app.version";
	
	// logging properties
	public static final String AWSTOOLS_LOG_PROPERTY_NAME = "log.name"; // env variable name used in logback.xml 
	public static final String AWSTOOLS_LOG_DIR = "logs";
	public static final String AWSTOOLS_LOG_FILE = Env.USER_HOME_DIR + Env.FILE_SEPERATOR + AWSTOOLS_HOME_DIR_NAME + Env.FILE_SEPERATOR + 
			AWSTOOLS_LOG_DIR + Env.FILE_SEPERATOR + "application-" + DateTimeUtil.dateSDF.format( System.currentTimeMillis() ) + ".log";
	public static final String LOGBACK_CONSOLE_OUTPUT_LOGGER = "stdout";
	public static final String LOGBACK_FILE_OUTPUT_LOGGER = "fout";
	
	// config file property names
	public static final String PROP_ACCESS_KEY = "key";
	public static final String PROP_SECRET_KEY = "secret";
	public static final String PROP_REGION = "region";
}
