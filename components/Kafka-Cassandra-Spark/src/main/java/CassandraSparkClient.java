import java.util.Date;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;



public class CassandraSparkClient
{
	private static int timeWindowInSeconds = 3 * 60; // three minutes
	private static int topNLanguages = 3;
	private static int topNNations = 3;



	private static JavaSparkContext setupJavaSparkContext()
	{
		SparkConf conf = new SparkConf();
        conf.setAppName("CassandraSparkQueryClient");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "127.0.0.1");

        return new JavaSparkContext(conf);
	}



	public static void main(String[] args)
	{
        JavaSparkContext jsc = setupJavaSparkContext();

    	Date startingDate = null;

        CassandraSparkQueryFunctions queryFunctions =
        	new CassandraSparkQueryFunctions(startingDate);

        while(true)
        {
        	startingDate = getSecondsAgoDate(timeWindowInSeconds);

		    queryFunctions.setStartingDate(startingDate);

		    CassandraTableScanJavaRDD<CassandraRow> table =
		    	CassandraJavaUtil
		    	.javaFunctions(jsc)
		    	.cassandraTable("giteventskeyspace", "events");

		    printTopLanguages(table);
		    printTopNations(table);
        }
	}



	private static Date getSecondsAgoDate(int seconds)
	{
		return new Date(System.currentTimeMillis() - seconds * 1000);
	}



	private static void printTopLanguages(CassandraTableScanJavaRDD<CassandraRow> table)
	{
	    List<Tuple2<String, Long>> topLanguages =
	    	table
    		.select("language", "timestamp")
	    	.filter(CassandraSparkQueryFunctions.filterAfterStartingDate)
	    	.groupBy(CassandraSparkQueryFunctions.groupByLanguage)
	    	.map(CassandraSparkQueryFunctions.countLanguages)
	    	.sortBy(CassandraSparkQueryFunctions.sortByLanguageCount, false, 1)
	    	.collect();

		System.out.println("The " + topNLanguages + " most popular languages : ");

	    for(int i = 0; i < Integer.min(topLanguages.size(), topNLanguages); i++)
	    	System.out.println(topLanguages.get(i));

	    System.out.println();
	}



	private static void printTopNations(CassandraTableScanJavaRDD<CassandraRow> table)
	{
	    List<Tuple2<String, Long>> topNations =
	    	table
    		.select("nation", "timestamp")
	    	.filter(CassandraSparkQueryFunctions.filterAfterStartingDate)
	    	.groupBy(CassandraSparkQueryFunctions.groupByNation)
	    	.map(CassandraSparkQueryFunctions.countNations)
	    	.sortBy(CassandraSparkQueryFunctions.sortByNationCount, false, 1)
	    	.collect();

		System.out.println("The " + topNNations + " most active nations : ");

	    for(int i = 0; i < Integer.min(topNations.size(), topNNations); i++)
	    	System.out.println(topNations.get(i));

	    System.out.println();
	}
}
