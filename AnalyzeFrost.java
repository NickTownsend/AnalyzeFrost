package edu.easternct.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AnalyzeFrost {
	
public static void main(String[] args) throws Exception {

	    
	    SparkConf conf = new SparkConf().setAppName("AnalyzeFrost");
	    conf.setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String>	mydata = sc.textFile(args[1]);

		
	    JavaRDD<String> mydataUPPER = mydata.map((String line) -> line.toUpperCase());   // l m n s t
	    JavaRDD<String> filterData = mydataUPPER.filter((String line)-> line.matches(".*[LMNST]{2}.*"));
	    long lineCount_ll =  filterData.count(); 
	    
	    System.out.println("This is the AnalyzeFrost Example code.");
	    
	    System.out.println(lineCount_ll);
	    
	    
	    sc.close();   
	    

	  }
}