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
import org.apache.commons.lang3.tuple.Pair;
import org.warn.aws.s3.model.S3OperationRecord;
import org.warn.aws.util.ConfigConstants;
import org.warn.aws.util.Constants;
import org.warn.aws.util.DataTransferUtil;
import org.warn.aws.util.ErrorHandler;
import org.warn.aws.util.ValidationsUtil;
import org.warn.utils.file.FileHelper;
import org.warn.utils.file.FileOperations;
import org.warn.utils.perf.PerformanceLogger;

import java.io.File;
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

    private final ExecutorService executorService;
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
        this.executorService = executorService;
        this.transferManager = TransferManagerBuilder.standard()
                .withS3Client( s3Client )
                .withMultipartUploadThreshold( (long) (100 * 1024 * 1024) )
                .withExecutorFactory( () -> executorService )
                .build();
    }

    public void putObject( String bucketName, String localFilePath, String s3PathPrefix, String optionType ) {
        PerformanceLogger performanceLogger = new PerformanceLogger();
        performanceLogger.start();
        AtomicLong totalBytes = new AtomicLong();
        AtomicInteger successfulCount = new AtomicInteger();
        List<S3OperationRecord> failedUploads = Collections.synchronizedList( new ArrayList<>() );
        Path initialPath = Paths.get( localFilePath );

        try {
            switch( optionType ) {
                case Constants.OPTION_FILE:
                    uploadSingleFile( bucketName, s3PathPrefix, initialPath.toFile(), totalBytes, successfulCount, failedUploads);
                    break;

                case Constants.OPTION_DIRECTORY:
                    uploadDirectory( bucketName, s3PathPrefix, initialPath, totalBytes, successfulCount, failedUploads);
                    break;

                case Constants.OPTION_LIST:
                    uploadFromList( bucketName, s3PathPrefix, localFilePath, totalBytes, successfulCount, failedUploads );
                    break;

                default:
                    ValidationsUtil.handleUnsupported( Constants.MSG_UNSUPPORTED_OPTION, optionType );
                    return;
            }
        } catch( AmazonS3Exception e ) {
            ErrorHandler.handleAmazonS3Exception( e, bucketName, localFilePath, initialPath, failedUploads );

        } catch( AmazonClientException e ) {
            ErrorHandler.handleAmazonClientException( e, bucketName, localFilePath, initialPath, failedUploads );

        } catch (InterruptedException e) {
            ErrorHandler.handleInterruptedException(e, initialPath);

        } finally {
            transferManager.shutdownNow();
        }

        postProcessing( failedUploads, successfulCount, totalBytes, performanceLogger );
    }

    private void uploadFromList( String bucketName, String s3PathPrefix, String localFilePath, AtomicLong totalBytes,
        AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) throws InterruptedException {
        List<String> fileNames = FileOperations.readLines(localFilePath);
        for(String fileName: fileNames) {
            uploadSingleFile( bucketName, s3PathPrefix, new File(fileName), totalBytes, successfulCount, failedUploads);
        }
    }

    private void uploadDirectory( String bucketName, String s3PathPrefix, Path initialPath, AtomicLong totalBytes,
        AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) throws InterruptedException {

        String directoryName = initialPath.subpath( 2, initialPath.getNameCount() ).toString();
        if( s3PathPrefix != null )
            directoryName = ValidationsUtil.formatPath( s3PathPrefix );
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
            checkUploadStatus( bucketName, ValidationsUtil.formatPath( directoryName ) + localFile.getName(), localFile.getAbsolutePath(),
                    fileSizeLocal, fileSizeS3, successfulCount, failedUploads );
        }
    }

    private void uploadSingleFile( String bucketName, String s3PathPrefix, File file, AtomicLong totalBytes,
        AtomicInteger successfulCount, List<S3OperationRecord> failedUploads ) throws InterruptedException {

        Path initialPath = Paths.get( file.getAbsolutePath() );
        String s3FileKey = initialPath.subpath( 2, initialPath.getNameCount() ).toString();
        if( s3PathPrefix != null )
            s3FileKey = ValidationsUtil.formatPath( s3PathPrefix ) + file.getName();

        Upload upload = transferManager.upload( bucketName, s3FileKey, file );
        upload.addProgressListener( getProgressListener( totalBytes ) );
        upload.waitForCompletion();

        long fileSizeLocal = file.length();
        long fileSizeS3 = upload.getProgress().getBytesTransferred();
        checkUploadStatus( bucketName, s3FileKey, file.getAbsolutePath(), fileSizeLocal, fileSizeS3,
                successfulCount, failedUploads );
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

                    float transferRate =  DataTransferUtil.calculateTransferRate(
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
        float transferRate = DataTransferUtil.calculateTransferRate( totalBytes.get(), performanceLogger.getLastCalculatedDuration() );

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

}
