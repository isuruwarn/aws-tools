package org.warn.aws;

import com.amazonaws.regions.Regions;
import lombok.extern.slf4j.Slf4j;
import org.warn.aws.s3.client.S3ClientWrapper;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.aws.util.ValidationsUtil;
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
        ValidationsUtil.checkArgsLength( args.length, 1 );

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

            ValidationsUtil.checkIfCredentialsAreProvided( accessKey, secretKey, region );

            if (Constants.COMMAND_S3.equals(command)) {
                ValidationsUtil.checkArgsLength(args.length, 5);

                String s3Operation = args[1];
                String bucketName = args[2];
                ValidationsUtil.validateOperation(s3Operation);
                ValidationsUtil.checkIfBucketNameIsProvided(bucketName);

                S3ClientWrapper s3ClientWrapper = new S3ClientWrapper( accessKey, secretKey, Regions.fromName(region),
                        executorService );

                if( Constants.OPERATION_PUT.equals( s3Operation ) ) {
                    String optionType = args[3];
                    ValidationsUtil.validateOption( optionType );

                    String localFilePath = args[4];
                    String s3PathPrefix = null;
                    if( args.length > 5 )
                        s3PathPrefix = args[5];

                    log.info("Initializing S3 {} operation - BucketName={}, s3PathPrefix={}",
                            s3Operation, bucketName, s3PathPrefix );
                    log.info("Option={}, fileOrDirectory={}", optionType, localFilePath);

                    s3ClientWrapper.putObject( bucketName, localFilePath, s3PathPrefix, optionType );
                }

            } else {
                ValidationsUtil.handleUnsupported( Constants.MSG_UNSUPPORTED_OPERATION, command );
            }

        } finally {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES );
        }

    }
}
