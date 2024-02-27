package edu.upf.model;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;

public class Bigram implements Serializable {
    private String word1;
    private String word2;

    // Constructor, getters, setters...

    public Bigram(String word1, String word2) {
        this.word1 = word1;
        this.word2 = word2;
    }

    // Other methods...

    public Tuple2<String,String> getBigram(){
        return new Tuple2<>(this.word1, this.word2);
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bigram bigram = (Bigram) o;
        return Objects.equals(word1, bigram.word1) &&
                Objects.equals(word2, bigram.word2);
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    // hashCode and other methods...
}