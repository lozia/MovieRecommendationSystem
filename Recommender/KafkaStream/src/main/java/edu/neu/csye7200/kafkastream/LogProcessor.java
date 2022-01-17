package edu.neu.csye7200.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;


public class LogProcessor implements Processor<byte[], byte[]>{

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        // Use string to represent the collected log information
        String input = new String(line);
        // According to the prefix MOVIE_RATING_PREFIX: extract scoring data from log information
        if( input.contains("MOVIE_RATING_PREFIX:") ){
            System.out.println("movie rating data coming!>>>>>>>>>>>" + input);

            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward( "logProcessor".getBytes(), input.getBytes() );
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}

