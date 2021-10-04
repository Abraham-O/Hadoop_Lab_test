package com.opstty;

import com.opstty.job.Distinct_Districts_Trees;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
/// adding my new class Distinct_Districts_Trees
            programDriver.addClass("Distinct_Districts_Trees", Distinct_Districts_Trees.class,
                    "A map/reduce program that returns the distinct districts with trees in a predefined CSV formatting.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
