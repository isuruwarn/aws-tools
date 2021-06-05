package org.warn.aws;

import lombok.extern.slf4j.Slf4j;
import org.warn.aws.s3.client.S3ClientWrapper;
import org.warn.aws.util.Constants;

@Slf4j
public class AwsTools {



    public static void main( String [] args ) {

        checkArgsLength( args.length, 1 );

        // check for credentials

        String command = args[0];
        if( Constants.COMMAND_S3.equals( command ) ) {

            checkArgsLength( args.length, 5 );

            String operation = args[1];
            String localFilePath = args[2];
            String bucketName = args[3];
            String key = args[4];

            validateOperation( operation );

            S3ClientWrapper s3ClientWrapper = new S3ClientWrapper();

            if( Constants.OPERATION_PUT.equals( operation ) ) {
                log.info("Initializing S3 {} - BucketName={}, Key={}", operation, bucketName, key);
                s3ClientWrapper.putObject( bucketName, key, localFilePath );
            }

        } else {
            handleUnsupportedOperation();
        }

    }

    private static void checkArgsLength( int argsLength, int expectedLength ) {
        if( argsLength < expectedLength ) {
            System.out.println( Constants.USAGE );
            System.exit(1);
        }
    }

    private static void validateOperation( String operation ) {
        if( !Constants.SUPPORTED_OPERATIONS_LIST.contains( operation ) )
            handleUnsupportedOperation();
    }

    private static void handleUnsupportedOperation() {
        System.out.println( Constants.MSG_UNSUPPORTED );
        System.out.println( Constants.USAGE );
        System.exit(1);
    }
}
