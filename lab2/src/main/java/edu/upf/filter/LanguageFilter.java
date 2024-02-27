package edu.upf.filter;

import org.apache.spark.api.java.JavaRDD;

public interface LanguageFilter {

  /**
   * Process
   * @param language
   * @return
   */
  Iterable<String> filterLanguage(JavaRDD<String> inputRDD, String language) throws Exception;
}
