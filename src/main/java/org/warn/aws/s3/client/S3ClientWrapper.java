package org.warn.aws.s3.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.warn.aws.s3.model.S3OperationRecord;
import org.warn.aws.util.Constants;

import java.io.File;
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
import java.util.stream.Stream;

@Slf4j
public class S3ClientWrapper {

    private final AWSCredentials credentials;
    private final AmazonS3 s3Client;
    private final ExecutorService executorService;

    public S3ClientWrapper( String accessKey, String secretKey, Regions region, ExecutorService executorService ) {
        this.credentials = new BasicAWSCredentials( accessKey,secretKey );
        this.s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials( new AWSStaticCredentialsProvider( credentials ) )
                .withRegion( region )
                .build();
        this.executorService = executorService;
    }

    public void putObject( String bucketName, String localFilePath ) {
        AtomicInteger successfulCount = new AtomicInteger();
        List<S3OperationRecord> failedUploads = Collections.synchronizedList( new ArrayList<>() );
        List<CompletableFuture<Void>> cfList = new ArrayList<>();

//        // DOES NOT WORK UNLESS USER IS BUCKET OWNER
//        String accelerateStatus = s3Client.getBucketAccelerateConfiguration(
//                    new GetBucketAccelerateConfigurationRequest(bucketName) )
//                .getStatus();
//        System.out.println("Bucket accelerate status: " + accelerateStatus);

        Path initialPath = Path.of( localFilePath );
        try( Stream<Path> files = Files.walk( initialPath ) ) {
            files.forEach( path -> {
                File file = path.toFile();
                if( file.isFile() ) {
                    String key = path.toString().replace( initialPath.getParent().toString() + "/", "" );
                    CompletableFuture<Void> cf = CompletableFuture.runAsync( () -> {
                        putObject( bucketName, key, file, successfulCount, failedUploads );
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
            // write to failed uploads log
        }

        log.info("---------------------------------------");
        log.info("S3 Upload Summary");
        log.info("---------------------------------------");
        log.info("Successful Object(s): " + successfulCount.get());
        log.info("Failed Object(s): " + failedUploads.size());
    }

    private void putObject( String bucketName, String key, File localFilePath, AtomicInteger successfulCount,
        List<S3OperationRecord> retryList ) {

        try {
            log.info("S3 upload - Key={}", key);
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
                retryList.add( new S3OperationRecord() );

            } else {
                successfulCount.getAndIncrement();
                log.info( "S3 upload successful - file={}, etag={}, fileSizeS3={}, fileSizeLocal={}",
                        key, eTag, fileSizeS3, fileSizeLocal );
            }

        } catch( AmazonS3Exception e ) {

            String errorMsg = null;
            if( e.getErrorCode().equals("PermanentRedirect") ) { // StatusCode: 301
                errorMsg = Constants.MSG_INCORRECT_REGION;

            } else if( e.getErrorCode().equals("InvalidAccessKeyId") ) { // StatusCode: 403
                errorMsg = Constants.MSG_INVALID_ACCESS_KEY;

            } else if( e.getErrorCode().equals("SignatureDoesNotMatch") ) { // StatusCode: 403
                errorMsg = Constants.MSG_INVALID_SECRET_KEY;

            } else if( e.getErrorCode().equals("NoSuchBucket") ) { // StatusCode: 403
                errorMsg = Constants.MSG_INVALID_BUCKET_NAME;
            }

            if( errorMsg != null ) {
                System.err.println();
                System.err.println( errorMsg );
                System.exit(1); // no point continuing since all requests will fail due to above errors
            }

            log.error( "Error while uploading S3 object - Key={}, StatusCode={}, ErrorCode={}",
                    key, e.getStatusCode(), e.getErrorCode() );
            retryList.add( new S3OperationRecord() );

        } catch( AmazonClientException e ) {
            if( ExceptionUtils.indexOfType( e, UnknownHostException.class) == 1 ) {
                System.err.println();
                System.err.println( Constants.MSG_NO_CONNECTIVITY );
                System.exit(1);
            }
            log.error( "Error while uploading S3 object - key={}, Error={}", key, e.getMessage() );
            retryList.add( new S3OperationRecord() );
        }
    }

}
