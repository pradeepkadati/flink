package io.kbeans.flink.DataStreaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;

public class AveragePassengers {

	
	
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> cabData = env.readTextFile("/home/pradeep/Learnings/Flink/assignment/cab.txt");
		
		DataSet<Tuple8<String,String,String,String,String,String,String,Integer>> onGoingTrips = cabData.map(new MapFunction<String,Tuple8<String,String,String,String,String,String,String,Integer>>() {

			@Override
			public Tuple8<String,String,String,String,String,String,String,Integer> map(String value) throws Exception {
				
				String[] trip =  value.split(",");
				Tuple8<String, String, String, String, String, String, String, Integer> tuple = null;
				
				if (trip[4].equals("yes")) {
				  tuple = new Tuple8<String, String, String, String, String, String, String, Integer>(trip[0], trip[1], 
							trip[2], trip[3], trip[4], trip[5], trip[6], Integer.parseInt(trip[7]));
				}else {
				  tuple = new Tuple8<String, String, String, String, String, String, String, Integer>(trip[0], trip[1], 
								trip[2], trip[3], trip[4], trip[5], trip[6], 0);
				}
				return tuple;
			}
		}).filter(new FilterFunction<Tuple8<String,String,String,String,String,String,String,Integer>>() {
			
			@Override
			public boolean filter(Tuple8<String, String, String, String, String, String, String, Integer> value)
					throws Exception {
				
				return value.f4.equals("yes");
			}
		});
		
		DataSet<Tuple2<String,Double>> avgPassengers = onGoingTrips.map(new MapFunction<Tuple8<String,String,String,String,String,String,String,Integer>, Tuple3<String,Integer,Integer>>() {

			@Override
			public Tuple3<String, Integer, Integer> map(
					Tuple8<String, String, String, String, String, String, String, Integer> value) throws Exception {
				
				return new Tuple3<String, Integer, Integer>(value.f6,value.f7,1);
			}
		}).groupBy(0).reduce(new ReduceFunction<Tuple3<String,Integer,Integer>>() {
			
			@Override
			public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1,
					Tuple3<String, Integer, Integer> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple3<String, Integer, Integer>(v1.f0,v1.f1+v2.f1,v1.f2+v2.f2);
			}
		}).map(new MapFunction<Tuple3<String,Integer,Integer>, Tuple2<String,Double>>() {

			@Override
			public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Double>(value.f0, ((value.f1*1.0)/value.f2));
			}
		});
		
		avgPassengers.writeAsText("/home/pradeep/Learnings/Flink/assignment/avgPassengers.txt");
	
		env.execute("Avg Passengers per Trip");
	}
}
