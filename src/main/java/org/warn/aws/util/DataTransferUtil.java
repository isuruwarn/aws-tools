package org.warn.aws.util;

public class DataTransferUtil {

    public static float calculateTransferRate( long bytesTransferred, long durationInSeconds ) {
        float transferRate = 0;
        if( durationInSeconds > 0 ) {
            float megaBitsTransferred = (bytesTransferred * 8f) / (1024f * 1024f);
            transferRate = megaBitsTransferred / durationInSeconds; // Mbps
        }
        return transferRate;
    }
}
