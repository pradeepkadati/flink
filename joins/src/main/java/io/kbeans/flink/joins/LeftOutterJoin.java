package io.kbeans.flink.joins;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class LeftOutterJoin {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final ParameterTool params = ParameterTool.fromArgs(args);
// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

// Read person file and generate tuples out of each string read
		DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() // presonSet = tuple of (1 John)
				{
					public Tuple2<Integer, String> map(String value) {
						String[] words = value.split(","); // words = [ {1} {John}]
						return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
					}
				});
// Read location file and generate tuples out of each string read
		DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
				.map(new MapFunction<String, Tuple2<Integer, String>>() { // locationSet = tuple of (1 DC)
					public Tuple2<Integer, String> map(String value) {
						String[] words = value.split(",");
						return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
					}
				});

// join datasets on person_id
// joined format will be <id, person_name, state>
		DataSet<Tuple3<Integer, String, String>> joined = personSet.leftOuterJoin(locationSet).where(0).equalTo(0).with(
				new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

					public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
							Tuple2<Integer, String> location) {
						 
						if(location == null) {
							return new Tuple3<Integer, String, String>(person.f0, person.f1, "NA"); 
						 }
						
						return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1); // returns tuple
																										// of (1 John
																										// DC)
					}
				});

		joined.writeAsCsv(params.get("output"), "\n", " ");

		env.execute("Join example");
	}
}
