package org.warn.aws;

import com.amazonaws.regions.Regions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.warn.aws.s3.client.S3ClientWrapper;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.utils.config.PropertiesHelper;
import org.warn.utils.config.UserConfig;
import org.warn.utils.perf.PerformanceLogger;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AwsTools {

    static {
        System.setProperty( ConfigConstants.AWSTOOLS_LOG_PROPERTY_NAME, ConfigConstants.AWSTOOLS_LOG_FILE );
    }

    private static final int MAX_CONCURRENT_TASKS = 20;

//    private static final Logger log = LoggingUtils.getFileOutputLogger();
//    private static final Logger clog = LoggingUtils.getConsoleLogger();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_TASKS);
    private static final UserConfig userConfig = new UserConfig( null, ConfigConstants.AWSTOOLS_HOME_DIR_NAME, ConfigConstants.AWSTOOLS_CONFIG_FILE );

    public static void main( String [] args ) throws InterruptedException {

        // initial argument length check
        checkArgsLength( args.length, 1 );

        PerformanceLogger pl = new PerformanceLogger();
        pl.start();

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
                System.out.println("");
                System.err.println( Constants.MSG_CONFIGURE_CREDENTIALS );
                System.out.println( Constants.USAGE );
                System.exit(1);
            }

            if (Constants.COMMAND_S3.equals(command)) {

                checkArgsLength(args.length, 4);

                String s3Operation = args[1];
                String localFilePath = args[2];
                String bucketName = args[3];

                validateOperation(s3Operation);

                S3ClientWrapper s3ClientWrapper = new S3ClientWrapper( accessKey, secretKey, Regions.fromName(region),
                        executorService );

                if( Constants.OPERATION_PUT.equals( s3Operation ) ) {
                    log.info("Initializing S3 {} operation - BucketName={}", s3Operation, bucketName );
                    s3ClientWrapper.putObject( bucketName, localFilePath );
                }

            } else {
                handleUnsupportedOperation();
            }

        } finally {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES );
            pl.printStatistics();
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
