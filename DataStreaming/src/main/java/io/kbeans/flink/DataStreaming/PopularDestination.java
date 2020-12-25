package io.kbeans.flink.DataStreaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;

public class PopularDestination {

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> cabData = env.readTextFile("/home/pradeep/Learnings/Flink/assignment/cab.txt");
		
		DataSet<Tuple8<String,String,String,String,String,String,String,String>> mappedCabData = cabData.map(new MapFunction<String,Tuple8<String,String,String,String,String,String,String,String>>() {

			@Override
			public Tuple8<String,String,String,String,String,String,String,String> map(String value) throws Exception {
				
				String[] trip =  value.split(",");
				
				return new Tuple8<String, String, String, String, String, String, String, String>(trip[0], trip[1], 
						trip[2], trip[3], trip[4], trip[5], trip[6], trip[7]);
			}
		});
		
		
		DataSet<Tuple8<String,String,String,String,String,String,String,String>> onGoingTrip = mappedCabData.filter(new FilterFunction<Tuple8<String,String,String,String,String,String,String,String>>() {
			
			@Override
			public boolean filter(Tuple8<String, String, String, String, String, String, String, String> value)
					throws Exception {
				
				 if (value.f4.equals("yes")) {
					 return true;
				 }
				
				return false;
			}
		});
		
		DataSet<Tuple2<String,Integer>> destinationCountMap = onGoingTrip.map(new MapFunction<Tuple8<String,String,String,String,String,String,String,String>, Tuple2<String,Integer>>() {

			@Override
			public Tuple2<String, Integer> map(
					Tuple8<String, String, String, String, String, String, String, String> value) throws Exception {
				
				return new Tuple2<String, Integer>(value.f6,Integer.parseInt(value.f7));
			}
		});
		
		
		DataSet<Tuple2<String,Integer>> popularDestination = destinationCountMap.groupBy(0).sum(1).maxBy(1);
		
		popularDestination.writeAsText("/home/pradeep/Learnings/Flink/assignment/pd.txt");
		
		
		env.execute("Cab - Find Popular Destinations");
	}
	
	

}
