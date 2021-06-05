package org.warn.aws.s3.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class S3OperationRecord {

    private String operation;
    private String bucketName;
    private String key;
    private String localFilePath;
}
