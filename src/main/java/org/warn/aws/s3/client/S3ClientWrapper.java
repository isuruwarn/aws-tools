package org.warn.aws.s3.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.warn.aws.s3.model.S3OperationRecord;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.utils.file.FileHelper;
import org.warn.utils.file.FileOperations;
import org.warn.utils.perf.PerformanceLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Slf4j
public class S3ClientWrapper {

    private final AmazonS3 s3Client;
    private final ExecutorService executorService;

    public S3ClientWrapper( String accessKey, String secretKey, Regions region, ExecutorService executorService ) {
        AWSCredentials credentials = new BasicAWSCredentials( accessKey, secretKey );
        this.s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials( new AWSStaticCredentialsProvider( credentials ) )
                .withRegion( region )
                .withAccelerateModeEnabled( true )
                .build();
        this.executorService = executorService;
    }

    public void putObject( String bucketName, String localFilePath, String s3PathPrefix ) {
        PerformanceLogger performanceLogger = new PerformanceLogger();
        performanceLogger.start();

        AtomicLong totalBytes = new AtomicLong();
        AtomicInteger successfulCount = new AtomicInteger();
        List<S3OperationRecord> failedUploads = Collections.synchronizedList( new ArrayList<>() );
        List<CompletableFuture<Void>> cfList = new ArrayList<>();
        Path initialPath = Path.of( localFilePath );

        try( Stream<Path> files = Files.walk( initialPath ) ) {
            files.forEach( path -> {
                File f = path.toFile();
                if( f.isFile() ) {
                    String key = formatPathPrefix( s3PathPrefix ) +
                            path.toString().replace( initialPath.getParent().toString() + "/", "" );
                    CompletableFuture<Void> cf = CompletableFuture.runAsync( () -> {
                        putObject( bucketName, key, f, successfulCount, failedUploads, totalBytes );
                    }, executorService);
                    cfList.add(cf);
                }
            });
        } catch( IOException e ) {
            System.err.println();
            System.err.println( Constants.MSG_INVALID_FILEPATH );
            System.exit(1);
        }
        CompletableFuture.allOf( cfList.toArray( new CompletableFuture[0] ) ).join();

        if( failedUploads.size() > 0 ) {

            StringBuilder csv = new StringBuilder();
            csv.append( ConfigConstants.FAILED_S3_UPLOADS_CSV_HEADER_ROW );
            for( S3OperationRecord r: failedUploads ) {
                csv.append( r.toCsvString( ConfigConstants.ERROR_LOGS_CSV_DELIMITER ) );
            }

            // write failed records to error log
            if( FileOperations.checkOrCreateDirInHomeDir( ConfigConstants.ERROR_LOGS_DIR ) ) {
                FileOperations.appendToFileInHomeDir( csv.toString(), ConfigConstants.FAILED_S3_UPLOADS_CSV );
            }
        }
        performanceLogger.printStatistics();
        long duration = Math.max( performanceLogger.getLastCalculatedDuration(), 1 ); // avoid division by zero
        double mbTransferred = totalBytes.get() * 8 / ( 1024 * 1024 );
        double transferRate =  mbTransferred / duration;

        log.info("S3 Upload Summary");
        log.info("---------------------------------------");
        log.info("Successful Object(s): {}", successfulCount.get());
        log.info("Failed Object(s): {}", failedUploads.size());
        log.info("Total Data Transferred: {}", FileHelper.printFileSizeUserFriendly( totalBytes.get() ) );
        log.info("Transfer Rate: {} (Mbps)", transferRate );
        log.info("---------------------------------------");
    }

    private void putObject( String bucketName, String key, File localFilePath, AtomicInteger successfulCount,
                            List<S3OperationRecord> failedUploads, AtomicLong totalBytes ) {

        try {
            log.info("S3 upload - Key={}, ThreadName={}, ThreadId={}",
                    key, Thread.currentThread().getName(), Thread.currentThread().hashCode() );
            PutObjectResult result = s3Client.putObject( bucketName, key, localFilePath );
            String eTag = result.getETag();

            // verify
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest( bucketName, key );
            final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(metadataRequest);
            long fileSizeS3 = objectMetadata.getContentLength();
            long fileSizeLocal = localFilePath.length();

            if( fileSizeLocal != fileSizeS3 ) {
                log.error( "S3 upload FAILED - file={}, etag={}, fileSizeS3={}, fileSizeLocal={}",
                        key, eTag, fileSizeS3, fileSizeLocal );
                failedUploads.add( new S3OperationRecord() );

            } else {
                successfulCount.getAndIncrement();
                totalBytes.getAndAdd( fileSizeS3 );
                log.info( "S3 upload successful - file={}, etag={}, fileSizeS3={}, fileSizeLocal={}",
                        key, eTag, fileSizeS3, fileSizeLocal );
            }

        } catch( AmazonS3Exception e ) {

            String errorMsg;
            switch( e.getErrorCode() ) {
                case "PermanentRedirect": // StatusCode: 301
                    errorMsg = Constants.MSG_INCORRECT_REGION;
                    break;

                case "InvalidAccessKeyId": // StatusCode: 403
                    errorMsg = Constants.MSG_INVALID_ACCESS_KEY;
                    break;

                case "SignatureDoesNotMatch": // StatusCode: 403
                    errorMsg = Constants.MSG_INVALID_SECRET_KEY;
                    break;

                case "NoSuchBucket": // StatusCode: 403
                    errorMsg = Constants.MSG_INVALID_BUCKET_NAME;
                    break;

                default:
                    errorMsg = null;
            }

            if( errorMsg != null ) {
                System.err.println();
                System.err.println( errorMsg );
                System.exit(1); // no point continuing since all requests will fail due to above error
            }

            log.error( "Error during S3 upload - Key={}, StatusCode={}, ErrorCode={}",
                    key, e.getStatusCode(), e.getErrorCode() );
            failedUploads.add( new S3OperationRecord( bucketName, "N/A", localFilePath.getAbsolutePath(), -1, errorMsg ) );

        } catch( AmazonClientException e ) {
            if( ExceptionUtils.indexOfType( e, UnknownHostException.class ) == 1 ) {
                System.err.println();
                System.err.println( Constants.MSG_NO_CONNECTIVITY );
                System.exit(1);

            } else if( ExceptionUtils.indexOfType( e, FileNotFoundException.class ) == 1 ) {
                System.err.println();
                System.err.println( Constants.MSG_INVALID_FILEPATH );
                System.exit(1);
            }

            log.error( "Error during S3 upload - key={}, Error={}", key, e.getMessage() );
            failedUploads.add( new S3OperationRecord( bucketName, "N/A", localFilePath.getAbsolutePath(), -1, e.getMessage() ) );

        }
    }

    private String formatPathPrefix( String s3PathPrefix ) {
        if( s3PathPrefix == null )
            return "";
        if( !s3PathPrefix.endsWith("/") )
            s3PathPrefix = s3PathPrefix + "/";
        return s3PathPrefix;
    }

}
