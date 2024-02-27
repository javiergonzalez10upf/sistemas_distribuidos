package edu.upf;

import edu.upf.filter.FileLanguageFilter;
import edu.upf.uploader.S3Uploader;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TwitterFilter {
    public static void main( String[] args ) throws Exception {
        long start = System.currentTimeMillis();
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
        for(String inputFile: argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);
            final FileLanguageFilter filter = new FileLanguageFilter(inputFile, outputFile);
            filter.filterLanguage(language);
        }
        final S3Uploader uploader = new S3Uploader(bucket, language, Region.US_EAST_1);
        uploader.upload(Arrays.asList(outputFile));
        long end = System.currentTimeMillis();
        System.out.println("\nTime spent: " + (end-start));
    }
}
