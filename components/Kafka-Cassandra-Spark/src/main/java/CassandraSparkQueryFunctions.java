import java.util.Date;

import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraRow;

import scala.Tuple2;



public class CassandraSparkQueryFunctions
{
	
	
	private static Date startingDate;	
	
	
	
	public CassandraSparkQueryFunctions(Date startingDate)
	{
		CassandraSparkQueryFunctions.startingDate = startingDate;
	}
	
	
	
	public void setStartingDate(Date startingDate)
	{
		this.startingDate = startingDate;
	}
	
	
	
	public static Function<CassandraRow, Boolean> filterAfterStartingDate = 
    	new Function<CassandraRow, Boolean>()
    	{
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(CassandraRow row) throws Exception {
				return row.getDate("timestamp").after(startingDate);
			}
    	};
	            	
	    	
	    	
	public static Function<CassandraRow, String> groupByLanguage =
		new Function<CassandraRow, String>()
    	{
			private static final long serialVersionUID = 1L;

			@Override
			public String call(CassandraRow row) throws Exception {
				return row.getString("language");
			}
    	};
    	
    	
    	
	public static Function<CassandraRow, String> groupByNation =
			new Function<CassandraRow, String>()
	    	{
				private static final long serialVersionUID = 1L;

				@Override
				public String call(CassandraRow row) throws Exception {
					return row.getString("nation");
				}
	    	};
	        	
	    	
	    	
	private static Function<Tuple2<String, Iterable<CassandraRow>>, Tuple2<String, Long>> countStrings =		
		new Function<Tuple2<String, Iterable<CassandraRow>>, Tuple2<String, Long>>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, Iterable<CassandraRow>> t)
					throws Exception {
				
				return new Tuple2<String, Long>(t._1, t._2.spliterator().getExactSizeIfKnown());
				
			}    				
		};   
		
		
		
	public static Function<Tuple2<String, Iterable<CassandraRow>>, Tuple2<String, Long>>
	countLanguages = countStrings;
	
	
	
	public static Function<Tuple2<String, Iterable<CassandraRow>>, Tuple2<String, Long>>
	countNations = countStrings;
	
			
			
	private static Function<Tuple2<String, Long>, Long> sortBy_2 =
		new Function<Tuple2<String, Long>, Long>()
    	{
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Tuple2<String, Long> t) throws Exception {
				return t._2;
			}
    	};

    	
    	
	public static Function<Tuple2<String, Long>, Long> sortByLanguageCount =
	sortBy_2;
	
	
	
	public static Function<Tuple2<String, Long>, Long> sortByNationCount =
	sortBy_2;
	
}
