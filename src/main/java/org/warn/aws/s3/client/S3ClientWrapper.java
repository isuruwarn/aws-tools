package org.warn.aws.s3.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.warn.aws.s3.model.S3OperationRecord;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class S3ClientWrapper {

    private static final int CONCURRENT_REQUESTS = 20;

    private final AWSCredentials credentials;
    private final AmazonS3 s3client;
    private final ExecutorService executorService;

    public S3ClientWrapper() {
        // TODO read credentials from file
        this( "AKIAIVYYMS4R2HZAXA3Q", "07Z1EoBmRwDyQKrXcopPo+8FL1LSmQthxrep05ZC", Regions.US_WEST_1 );
    }

    public S3ClientWrapper( String accessKey, String secretKey, Regions region ) {
        this.credentials = new BasicAWSCredentials( accessKey,secretKey );
        this.s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials( new AWSStaticCredentialsProvider( credentials ) )
                .withRegion( region )
                .build();
        // TODO should this be static? Should the cachedPool be used?
        this.executorService = Executors.newFixedThreadPool( CONCURRENT_REQUESTS );
    }

    public void putObject( String bucketName, String key, String localFilePath ) {

        List<S3OperationRecord> retryList = new ArrayList<>(); // TODO use thread-safe type

        // TODO check if directory has been provided
        // TODO retrieve all filenames within directory
        // TODO add all files to future list
        // TODO execute parallel put requests

//        CompletableFuture<Void> cf = CompletableFuture.runAsync( () -> {
//            putObject( bucketName, key, localFilePath, retryList );
//        }, executorService );

        Future<Void> f = executorService.submit( () -> {
            putObject( bucketName, key, localFilePath, retryList );
            return null;
        });

        try {
            f.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void putObject( String bucketName, String key, String localFilePath, List<S3OperationRecord> retryList ) {

        try {
            //log.info("S3 PutObject - BucketName={}, Key={}", bucketName, key);
            PutObjectResult result = s3client.putObject(
                    bucketName,
                    key,
                    new File(localFilePath)
            );

            System.out.println("ArchiveStatus: " + result.getMetadata().getArchiveStatus());
            System.out.println("ContentLength: " + result.getMetadata().getContentLength());
            System.out.println("ETag: " + result.getMetadata().getETag());
            System.out.println("LastModified: " + result.getMetadata().getLastModified());
            System.out.println("VersionId: " + result.getMetadata().getVersionId());
            System.out.println("StorageClass: " + result.getMetadata().getStorageClass());

        } catch( AmazonS3Exception e ) {
            // TODO add file to retry list
            e.printStackTrace(); // TODO remove stacktrace

        } catch( AmazonClientException e ) {
            // TODO check for java.net.UnknownHostException

            // TODO add file to retry list
            e.printStackTrace(); // TODO remove stacktrace

            retryList.add( new S3OperationRecord() );


        }
    }

}
