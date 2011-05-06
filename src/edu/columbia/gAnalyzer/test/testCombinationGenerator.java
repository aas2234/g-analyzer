package edu.columbia.gAnalyzer.test;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.columbia.gAnalyzer.util.CombinationGenerator;

public class testCombinationGenerator {

	public static void main(String [] args) {
		
		List<LongWritable> set = new LinkedList<LongWritable>();
		set.add(new LongWritable(1));
		set.add(new LongWritable(2));
		set.add(new LongWritable(3));
		set.add(new LongWritable(4));
		
		CombinationGenerator<LongWritable> generator = new CombinationGenerator<LongWritable>(set, 2);
		
        for(List<LongWritable> combination : generator) {
            String edge = new String(combination.get(0).get() + ":" + combination.get(1));
            System.out.println(edge);
        }
		
	}
}
