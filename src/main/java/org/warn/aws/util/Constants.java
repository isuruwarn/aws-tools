package org.warn.aws.util;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String USAGE =
            "Usage:\n" +
                    "    <resource> <operation>\n" +
                    "Supported Operations:\n" +
                    "   s3 put <localFilePath> <bucketName> <key>";
//                    "Where:\n" +
//                    "    operation - the Amazon S3 operation to execute.\n" +
//                    "    bucketName - the Amazon S3 bucket to create.\n" +
//                    "    key - the key to use.\n" ;

    public static final String COMMAND_S3 = "s3";

    public static final String OPERATION_PUT = "put";

    public static final String MSG_UNSUPPORTED = "Unsupported operation!";

    public static final List<String> SUPPORTED_OPERATIONS_LIST = Arrays.asList( OPERATION_PUT );
}
