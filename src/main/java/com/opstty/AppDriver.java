package com.opstty;

import com.opstty.job.Distinct;
import com.opstty.job.WordCount;
import com.opstty.job.Species;
import com.opstty.job.NumberType;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("distinct_area", Distinct.class,
                    "A map reduce program that counts the number of arrondissements with trees");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that identifies all species represented in the dataset");
            programDriver.addClass("number_type", NumberType.class,
                    "A map/reduce program that lets the user count the various types of trees");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
