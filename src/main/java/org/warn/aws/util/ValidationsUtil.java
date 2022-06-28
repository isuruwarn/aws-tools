package org.warn.aws.util;

import org.apache.commons.lang3.StringUtils;

public class ValidationsUtil {

    public static void checkArgsLength( int argsLength, int expectedLength ) {
        if( argsLength < expectedLength ) {
            System.out.println( Constants.USAGE );
            System.exit(1);
        }
    }

    public static void checkIfCredentialsAreProvided( String accessKey, String secretKey, String region ) {
        if( StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey) || StringUtils.isEmpty(region) ) {
            System.out.println();
            System.err.println( Constants.MSG_CONFIGURE_CREDENTIALS );
            System.out.println( Constants.USAGE );
            System.exit(1);
        }
    }

    public static void checkIfBucketNameIsProvided( String bucketName ) {
        if( StringUtils.isEmpty(bucketName) ) {
            System.out.println();
            System.err.println( Constants.MSG_INVALID_BUCKET_NAME );
            System.out.println( Constants.USAGE );
            System.exit(1);
        }
    }

    public static void validateOperation( String operation ) {
        if( !Constants.SUPPORTED_OPERATIONS_LIST.contains( operation ) )
            handleUnsupported( Constants.MSG_UNSUPPORTED_OPERATION, operation );
    }

    public static void validateOption( String option ) {
        if( !Constants.SUPPORTED_OPTIONS_LIST.contains( option ) )
            handleUnsupported( Constants.MSG_UNSUPPORTED_OPTION, option );
    }

    public static void handleUnsupported( String message, String operation ) {
        System.out.println();
        System.err.println( message + operation );
        System.out.println();
        System.out.println( Constants.USAGE );
        System.exit(1);
    }

    public static String formatPath(String path ) {
        if( path == null )
            return "";
        if( !path.endsWith("/") )
            path = path + "/";
        return path;
    }
}
