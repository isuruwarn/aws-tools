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
import java.util.concurrent.TimeUnit;

@Slf4j
public class AwsTools {

    private static final int MAX_CONCURRENT_TASKS = 20;
    private static final ExecutorService executorService = Executors.newFixedThreadPool( MAX_CONCURRENT_TASKS );
    private static final UserConfig userConfig = new UserConfig( null,
            ConfigConstants.AWSTOOLS_DIR_NAME, ConfigConstants.CONFIG_FILE);

    public static void main( String [] args ) throws InterruptedException {

        // initial argument length check
        checkArgsLength( args.length, 1 );

        try {
            String currentAppVersion = PropertiesHelper
                    .loadFromResourcesDir( ConfigConstants.APP_PROPERTY_FILE_NAME )
                    .getProperty( ConfigConstants.APP_VERSION );
            log.info("org.warn.awstools version [{}]", currentAppVersion );

            String accessKey;
            String secretKey;
            String region;

            String command = args[0];
            if( Constants.COMMAND_CREDENTIALS.equals(command) ) {
                Scanner scanner = new Scanner(System.in);

                System.out.println("Access Key:");
                accessKey = scanner.next();

                System.out.println("Secret Key:");
                secretKey = scanner.next();

                System.out.println("Region:");
                region = scanner.next();

                userConfig.updateConfig( ConfigConstants.PROP_ACCESS_KEY, accessKey );
                userConfig.updateConfig( ConfigConstants.PROP_SECRET_KEY, secretKey );
                userConfig.updateConfig( ConfigConstants.PROP_REGION, region );

                scanner.close();
                System.exit(1);
            }

            accessKey = userConfig.getProperty( ConfigConstants.PROP_ACCESS_KEY );
            secretKey = userConfig.getProperty( ConfigConstants.PROP_SECRET_KEY );
            region = userConfig.getProperty( ConfigConstants.PROP_REGION );
            log.info("Aws Region - " + region );

            if( StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(accessKey) ) {
                System.out.println();
                System.err.println( Constants.MSG_CONFIGURE_CREDENTIALS );
                System.out.println( Constants.USAGE );
                System.exit(1);
            }

            if (Constants.COMMAND_S3.equals(command)) {

                checkArgsLength(args.length, 4);

                String s3Operation = args[1];
                String bucketName = args[2];

                validateOperation(s3Operation);

                S3ClientWrapper s3ClientWrapper = new S3ClientWrapper( accessKey, secretKey, Regions.fromName(region),
                        executorService );

                if( Constants.OPERATION_PUT.equals( s3Operation ) ) {
                    String fourthArg = args[3];
                    boolean uploadFromList = false;
                    int localFilePathIndex = 3;
                    int s3PathPrefixIndex = 4;

                    if( Constants.OPTION_FILE.equals(fourthArg) ) {
                        uploadFromList = true;
                        localFilePathIndex++;
                        s3PathPrefixIndex++;
                    }

                    String localFilePath = args[localFilePathIndex];

                    String s3PathPrefix = null;
                    if( args.length > s3PathPrefixIndex )
                        s3PathPrefix = args[s3PathPrefixIndex];

                    log.info("Initializing S3 {} operation - BucketName={}, s3PathPrefix={}",
                            s3Operation, bucketName, s3PathPrefix );
                    s3ClientWrapper.putObject( bucketName, localFilePath, s3PathPrefix, uploadFromList );
                }

            } else {
                handleUnsupportedOperation();
            }

        } finally {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES );
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
        System.out.println();
        System.out.println( Constants.USAGE );
        System.exit(1);
    }
}
