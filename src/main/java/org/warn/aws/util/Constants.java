package org.warn.aws.util;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String USAGE =
//            "Name:\n" +
//            "   awstools - Client for managing AWS resources\n\n" +
            "\nUsage:\n" +
            "   credentials <accessKey> <secretKey> <region>\n" +
            "   s3 put <localFilePath> <bucketName> <key>\n";

    public static final String COMMAND_AWSTOOLS = "awstools";
    public static final String COMMAND_CREDENTIALS = "credentials";
    public static final String COMMAND_S3 = "s3";

    public static final String OPERATION_PUT = "put";

    public static final String MSG_UNSUPPORTED = "Unsupported operation!";
    public static final String MSG_CONFIGURE_CREDENTIALS = "Please configure the AWS credentials!";
    public static final String MSG_NO_CONNECTIVITY = "Cannot connect to host. Please check internet connectivity!";

    public static final List<String> SUPPORTED_OPERATIONS_LIST = Arrays.asList( OPERATION_PUT );
}
