package edu.upf.uploader;

import java.util.List;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.File;

public class S3Uploader implements Uploader {
    private final String bucketName;
    private final String prefix;
    private final S3Client client;

    public S3Uploader(String bucketName, String prefix, Region region) {
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.client = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
    @Override
    public void upload(List<String> files) {

        int number_files = files.size();
        int count = 0;

        // Upload each file to the specified key (prefix) in the bucket

        for (String filePath : files) {

            String fileName = String.format("%s/part-00000", filePath);

            String key = prefix + "/" + new File(fileName).getName();
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            PutObjectResponse response = client.putObject(request, new File(fileName).toPath());

            if (response.sdkHttpResponse().isSuccessful()) {
                System.out.println("Successful uploaded file " + fileName + ". ETag: " + response.eTag());
                count ++;
            } else {
                System.out.println("Error uploading file " + fileName + ". Exception: " + response.sdkHttpResponse().statusCode());
            }
        }
        System.out.println("Number of files uploaded: " + count + "/" + number_files);
        client.close();
    }
}
