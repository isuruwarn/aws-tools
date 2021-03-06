package org.warn.aws.util;

import org.warn.utils.core.Env;
import org.warn.utils.datetime.DateTimeUtil;

public class ConfigConstants {

	public static final String AWSTOOLS_DIR_NAME = ".awstools";
	public static final String CONFIG_FILE = "config.json";

	public static final String APP_PROPERTY_FILE_NAME = "application.properties";
	public static final String APP_VERSION = "app.version";
	
	// config file property names
	public static final String PROP_ACCESS_KEY = "key";
	public static final String PROP_SECRET_KEY = "secret";
	public static final String PROP_REGION = "region";

	// error log properties
	public static final String ERROR_LOGS_DIR = AWSTOOLS_DIR_NAME + Env.FILE_SEPERATOR + "error-logs";
	public static final String ERROR_LOGS_CSV_DELIMITER = ",";
	public static final String FAILED_S3_UPLOADS_CSV =  ERROR_LOGS_DIR + Env.FILE_SEPERATOR +
			DateTimeUtil.dateSDF.format( System.currentTimeMillis() ) + "-s3-upload-failures.csv";
	public static final String FAILED_S3_UPLOADS_CSV_HEADER_ROW =
			"Bucket Name, Object Key, Local File Path, File Size, Error Message\n";
}
