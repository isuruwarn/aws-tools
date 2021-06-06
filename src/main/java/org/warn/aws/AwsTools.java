package org.warn.aws;

import com.amazonaws.regions.Regions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.warn.aws.s3.client.S3ClientWrapper;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.utils.config.PropertiesHelper;
import org.warn.utils.config.UserConfig;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class AwsTools {

    private static final int CONCURRENT_REQUESTS = 20;

//    private static final Logger log = LoggingUtils.getFileOutputLogger();
//    private static final Logger clog = LoggingUtils.getConsoleLogger();
    private static final ExecutorService executorService = Executors.newFixedThreadPool( CONCURRENT_REQUESTS );
    private static final UserConfig userConfig = new UserConfig( null, ConfigConstants.AWSTOOLS_HOME_DIR_NAME, ConfigConstants.AWSTOOLS_CONFIG_FILE );

    public static void main( String [] args ) {

        // initial argument length check
        checkArgsLength( args.length, 1 );

        try {
            String currentAppVersion = PropertiesHelper
                    .loadFromResourcesDir( ConfigConstants.APP_PROPERTY_FILE_NAME )
                    .getProperty( ConfigConstants.APP_VERSION );
            log.info("Initializing org.warn.AwsTools version " + currentAppVersion );

            String accessKey = null;
            String secretKey = null;
            String region = null;

            String command = args[0];
            if( Constants.COMMAND_CREDENTIALS.equals(command) ) {
                Scanner sc = new Scanner(System.in);

                System.out.println("Access Key:");
                accessKey = sc.next();

                System.out.println("Secret Key:");
                secretKey = sc.next();

                System.out.println("Region:");
                region = sc.next();

                userConfig.updateConfig( ConfigConstants.PROP_ACCESS_KEY, accessKey );
                userConfig.updateConfig( ConfigConstants.PROP_SECRET_KEY, secretKey );
                userConfig.updateConfig( ConfigConstants.PROP_REGION, region );

                sc.close();
                System.exit(1);
            }

            accessKey = userConfig.getProperty( ConfigConstants.PROP_ACCESS_KEY );
            secretKey = userConfig.getProperty( ConfigConstants.PROP_SECRET_KEY );
            region = userConfig.getProperty( ConfigConstants.PROP_REGION );

            if( StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(accessKey) ) {
                System.out.println("");
                System.err.println( Constants.MSG_CONFIGURE_CREDENTIALS );
                System.out.println( Constants.USAGE );
                System.exit(1);
            }


            if (Constants.COMMAND_S3.equals(command)) {

                checkArgsLength(args.length, 5);

                String s3Operation = args[1];
                String localFilePath = args[2];
                String bucketName = args[3];
                String key = args[4];

                validateOperation(s3Operation);

                S3ClientWrapper s3ClientWrapper = new S3ClientWrapper( accessKey, secretKey, Regions.fromName(region),
                        executorService );

                if (Constants.OPERATION_PUT.equals(s3Operation)) {
                    log.info("Initializing S3 {} - BucketName={}, Key={}", s3Operation, bucketName, key);
                    s3ClientWrapper.putObject(bucketName, key, localFilePath);
                }

            } else {
                handleUnsupportedOperation();
            }

        } finally {
            executorService.shutdown();
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
        System.out.println();
        System.err.println( Constants.MSG_UNSUPPORTED );
        System.out.println( Constants.USAGE );
        System.exit(1);
    }
}
