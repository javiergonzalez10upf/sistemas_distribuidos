package edu.upf.filter;

import edu.upf.parser.SimplifiedTweet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

public class FileLanguageFilter {
    final String inputFile;
    final String outputFile;

    public FileLanguageFilter(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    //@Override
    public void filterLanguage(String language) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true))) { //inputFile.toString() en las diapos

            String line;
            while ((line = reader.readLine()) != null) {
                Optional<SimplifiedTweet> optionalTweet = SimplifiedTweet.fromJson(line);
                if (optionalTweet.isPresent() && optionalTweet.get().getLanguage().equalsIgnoreCase(language)) {
                    writer.write(line + System.lineSeparator());
                }
            }
        } catch (IOException e) {
            throw new Exception("Error processing the file: " + e.getMessage(), e);
        }
    }

    public interface LanguageFilter {
        /**
         * Process
         * @param language
         * @return
         */
        void filterLanguage(String language) throws Exception;
    }
}
