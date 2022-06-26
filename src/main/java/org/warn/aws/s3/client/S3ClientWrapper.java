package org.warn.aws.s3.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.warn.aws.s3.model.S3OperationRecord;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.utils.file.FileHelper;
import org.warn.utils.file.FileOperations;
import org.warn.utils.perf.PerformanceLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class S3ClientWrapper {

    private final TransferManager transferManager;

    private LocalDateTime timeAtLastPrintout = LocalDateTime.now();
    private AtomicLong bytesAtLastPrintout = new AtomicLong();
    private AtomicReference<Float> minTransferRate = new AtomicReference<>( Float.MAX_VALUE );
    private AtomicReference<Float> maxTransferRate = new AtomicReference<>( 0.0f );

    public S3ClientWrapper( String accessKey, String secretKey, Regions region, ExecutorService executorService ) {
        AWSCredentials credentials = new BasicAWSCredentials( accessKey, secretKey );
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials( new AWSStaticCredentialsProvider( credentials ) )
                .withRegion( region )
                .withAccelerateModeEnabled( true )
                .build();
        this.transferManager = TransferManagerBuilder.standard()
                .withS3Client( s3Client )
                .withMultipartUploadThreshold( (long) (100 * 1024 * 1024) )
                .withExecutorFactory( () -> executorService )
                .build();
    }

    public void putObject( String bucketName, String localFilePath, String s3PathPrefix ) {
        PerformanceLogger performanceLogger = new PerformanceLogger();
        performanceLogger.start();
        AtomicLong totalBytes = new AtomicLong();
        AtomicInteger successfulCount = new AtomicInteger();
        List<S3OperationRecord> failedUploads = Collections.synchronizedList( new ArrayList<>() );
        Path initialPath = Paths.get( localFilePath );
        File file = initialPath.toFile();

        try {
            if( file.isDirectory() ) {
                uploadDirectory( bucketName, s3PathPrefix, initialPath, totalBytes, successfulCount, failedUploads);

            } else {
                uploadSingleFile( bucketName, file, totalBytes, localFilePath, successfulCount, failedUploads);
            }

        } catch( AmazonS3Exception e ) {
            handleAmazonS3Exception( e, bucketName, localFilePath, initialPath, failedUploads );

        } catch( AmazonClientException e ) {
            handleAmazonClientException( e, bucketName, localFilePath, initialPath, failedUploads );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error( "Error during S3 upload - Key={}, Message={}", initialPath, e.getMessage() );

        } finally {
            transferManager.shutdownNow();
        }
        postProcessing( failedUploads, successfulCount, totalBytes, performanceLogger );
    }

    private void uploadDirectory( String bucketName, String s3PathPrefix, Path initialPath, AtomicLong totalBytes,
        AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) throws InterruptedException {

        String directoryName = formatPathPrefix( s3PathPrefix ) +
                initialPath.getName( initialPath.getNameCount() - 1 ).toString();
        List<Pair<File, ObjectMetadata>> uploadMetadata = new ArrayList<>();

        MultipleFileUpload upload = transferManager.uploadDirectory(
                bucketName, directoryName, initialPath.toFile(), true,
                ( file1, objectMetadata ) -> uploadMetadata.add( Pair.of( file1, objectMetadata ) ) );
        upload.addProgressListener( getProgressListener( totalBytes ) );
        upload.waitForCompletion();

        for( Pair<File, ObjectMetadata> pair: uploadMetadata ) {
            File localFile = pair.getLeft();
            ObjectMetadata objectMetadata = pair.getRight();
            long fileSizeLocal = localFile.length();
            long fileSizeS3 = objectMetadata.getContentLength();
            checkUploadStatus( bucketName, "-", localFile.getAbsolutePath(), fileSizeLocal, fileSizeS3,
                    successfulCount, failedUploads );
        }
    }

    private void uploadSingleFile( String bucketName, File file,AtomicLong totalBytes, String localFilePath,
        AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) throws InterruptedException {

        Upload upload = transferManager.upload( bucketName, file.getName(), file );
        upload.addProgressListener( getProgressListener( totalBytes ) );
        upload.waitForCompletion();

        long fileSizeLocal = file.length();
        long fileSizeS3 = upload.getProgress().getBytesTransferred();
        checkUploadStatus( bucketName, file.getName(), localFilePath, fileSizeLocal, fileSizeS3,
                successfulCount, failedUploads );
    }

    private void handleAmazonS3Exception( AmazonS3Exception e, String bucketName, String localFilePath,
                                          Path initialPath, List<S3OperationRecord> failedUploads ) {
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
                initialPath, e.getStatusCode(), e.getErrorCode() );
        failedUploads.add( new S3OperationRecord( bucketName, "N/A", localFilePath, -1, errorMsg ) );
    }

    private void handleAmazonClientException( AmazonClientException e, String bucketName, String localFilePath,
                                              Path initialPath, List<S3OperationRecord> failedUploads ) {
        if( ExceptionUtils.indexOfType( e, UnknownHostException.class ) == 1 ) {
            System.err.println();
            System.err.println( Constants.MSG_NO_CONNECTIVITY );
            System.exit(1);

        } else if( ExceptionUtils.indexOfType( e, FileNotFoundException.class ) == 1 ) {
            System.err.println();
            System.err.println( Constants.MSG_INVALID_FILEPATH );
            System.exit(1);
        }

        log.error( "Error during S3 upload - key={}, Error={}", initialPath, e.getMessage() );
        failedUploads.add( new S3OperationRecord( bucketName, "N/A", localFilePath, -1, e.getMessage() ) );
    }

    private ProgressListener getProgressListener( final AtomicLong totalBytes ) {
        return ( progressEvent ) -> {
            long transferredBytes = progressEvent.getBytesTransferred();
            if( transferredBytes > 0 ) {
                totalBytes.getAndAdd(transferredBytes);

                // log progress every 30 seconds
                LocalDateTime now = LocalDateTime.now();
                long timeSinceLastOutput = ChronoUnit.SECONDS.between( timeAtLastPrintout, now );
                if( timeSinceLastOutput > 30 ) {

                    float transferRate =  calculateTransferRate(
                            totalBytes.get() - bytesAtLastPrintout.get(),  timeSinceLastOutput );
                    log.info("S3 Upload Transfer Progress: {}, Transfer Rate: {} (Mbps), ThreadId: {}",
                            FileHelper.printFileSizeUserFriendly( totalBytes.get() ), String.format("%.2f", transferRate),
                            Thread.currentThread().getId() );

                    timeAtLastPrintout = now;
                    bytesAtLastPrintout.set( totalBytes.get() );

                    if( transferRate < minTransferRate.get() )
                        minTransferRate.set( transferRate );
                    if( transferRate > maxTransferRate.get() )
                        maxTransferRate.set( transferRate );
                }
            }
        };
    }

    private void checkUploadStatus( String bucketName, String key, String localFilePath, long fileSizeLocal,
        long fileSizeS3, AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) {

        if( fileSizeLocal != fileSizeS3 ) {
            failedUploads.add( new S3OperationRecord( bucketName, key, localFilePath, fileSizeLocal, "-" ) );
            log.error("S3 upload FAILED - file={}, fileSizeLocal={}, fileSizeS3={}",
                    localFilePath, fileSizeLocal, fileSizeS3);
        } else {
            successfulCount.getAndIncrement();
            log.info("S3 upload successful - file={}, fileSizeLocal={}, fileSizeS3={}",
                    localFilePath, fileSizeLocal, fileSizeS3);
        }
    }

    private void postProcessing( List<S3OperationRecord> failedUploads, AtomicInteger successfulCount,
                                 AtomicLong totalBytes, PerformanceLogger performanceLogger ) {

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
        float transferRate = calculateTransferRate( totalBytes.get(), performanceLogger.getLastCalculatedDuration() );

        if( minTransferRate.get() == Float.MAX_VALUE || transferRate < minTransferRate.get() )
            minTransferRate.set( transferRate );

        if( maxTransferRate.get() == 0.0 || transferRate > maxTransferRate.get() )
            maxTransferRate.set( transferRate );

        log.info("S3 Upload Summary");
        log.info("---------------------------------------");
        log.info("Successful Object(s): {}", successfulCount.get());
        log.info("Failed Object(s): {}", failedUploads.size());
        log.info("Total Data Transferred: {}", FileHelper.printFileSizeUserFriendly( totalBytes.get() ) );
        log.info("Overall Transfer Rate: {} (Mbps)", String.format( "%.2f", transferRate ) );
        log.info("Minimum Transfer Rate: {} (Mbps)", String.format( "%.2f", minTransferRate.get() ) );
        log.info("Maximum Transfer Rate: {} (Mbps)", String.format( "%.2f", maxTransferRate.get() ) );
        log.info("---------------------------------------");
    }

    private String formatPathPrefix( String s3PathPrefix ) {
        if( s3PathPrefix == null )
            return "";
        if( !s3PathPrefix.endsWith("/") )
            s3PathPrefix = s3PathPrefix + "/";
        return s3PathPrefix;
    }

    private float calculateTransferRate( long bytesTransferred, long durationInSeconds ) {
        float transferRate = 0;
        if( durationInSeconds > 0 ) {
            float megaBitsTransferred = (bytesTransferred * 8f) / (1024f * 1024f);
            transferRate = megaBitsTransferred / durationInSeconds; // Mbps
        }
        return transferRate;
    }

}
