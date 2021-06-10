package org.warn.aws.s3.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class S3OperationRecord {

    private String bucketName;
    private String key;
    private String localFilePath;
    private long fileSize; // bytes
    private String errorMessage;

    public String toCsvString( String delimiter ) {
        return bucketName + delimiter + key + delimiter + localFilePath + delimiter +
                fileSize + delimiter + errorMessage + "\n";
    }
}
