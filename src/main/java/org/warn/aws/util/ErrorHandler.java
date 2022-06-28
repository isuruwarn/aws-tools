package org.warn.aws.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.warn.aws.s3.model.S3OperationRecord;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class ErrorHandler {

    public static void handleAmazonS3Exception(AmazonS3Exception e, String bucketName, String localFilePath,
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

    public static void handleAmazonClientException(AmazonClientException e, String bucketName, String localFilePath,
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

    public static void handleInterruptedException( InterruptedException e, Path initialPath ) {
        Thread.currentThread().interrupt();
        log.error( "Error during S3 upload - Key={}, Message={}", initialPath, e.getMessage() );
    }
}
