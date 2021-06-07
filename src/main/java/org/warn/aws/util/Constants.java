package org.warn.aws.util;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String USAGE =
            "Name:\n" +
            "   aws-tools - Client for managing AWS resources\n\n" +
            "\nUsage:\n" +
            "   credentials <accessKey> <secretKey> <region>\n" +
            "   s3 put <fileOrDirectoryPath> <bucketName>\n";

    public static final String COMMAND_CREDENTIALS = "credentials";
    public static final String COMMAND_S3 = "s3";

    public static final String OPERATION_PUT = "put";
    public static final String OPERATION_RETRY = "retry";

    public static final String MSG_UNSUPPORTED = "Unsupported operation!";
    public static final String MSG_CONFIGURE_CREDENTIALS = "Please configure the AWS credentials";
    public static final String MSG_INVALID_FILEPATH = "Please provide a valid file or directory";
    public static final String MSG_INVALID_BUCKET_NAME = "Please provide a valid S3 bucket name";
    public static final String MSG_INVALID_ACCESS_KEY = "Please configure valid Access Key";
    public static final String MSG_INVALID_SECRET_KEY = "Please configure valid Secret Key";
    public static final String MSG_INCORRECT_REGION = "Please configure correct region";
    public static final String MSG_NO_CONNECTIVITY = "Cannot connect to host. Please check internet connectivity";

    public static final List<String> SUPPORTED_OPERATIONS_LIST = Arrays.asList( OPERATION_PUT );
}
