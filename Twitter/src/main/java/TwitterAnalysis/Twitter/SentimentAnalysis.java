package TwitterAnalysis.Twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

public class SentimentAnalysis
{
	
	private final Logger LOG = Logger.getLogger(this.getClass());
	private static  String KAFKA_TOPIC ="twitter";
	protected static final String PositiveWords = null;
	static List<String> stopWords =new ArrayList<String>();
	private static Set<String> posWords=new HashSet<String>();
	private static Set<String> negWords=new HashSet<String>();
	
	public static void main(String[] args)
	{
		if (args.length < 6) {
			System.err.println("Usage: StockSparkApp <broker> <master> <in-topic> <out-topic> <cg> <interval>");
			System.err.println("eg: StockSparkApp localhost:9092 localhost:2181 test out mycg 5000");
			System.exit(1);
		}

		// set variables from command-line arguments
		final String broker = args[0];
		String master = args[1];
		KAFKA_TOPIC = args[2];
		String consumerGroup = args[4];
		long interval = Long.parseLong(args[5]);

		// define topic to subscribe to
		//final Pattern topicPattern = Pattern.compile(KAFKA_TOPIC, Pattern.CASE_INSENSITIVE);
		positiveWords();
		negativeWords();
		loadstopWords();
		
		List<String> topics=Arrays.asList(KAFKA_TOPIC);
		// set Kafka client parameters
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("group.id", consumerGroup);
		kafkaParams.put("enable.auto.commit", true);

		// initialize the streaming context
		JavaStreamingContext jssc = new JavaStreamingContext(master, "TwitterSentimentApp", new Duration(interval));

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(KAFKA_TOPIC, 1);
		final JavaInputDStream<ConsumerRecord<String, String>> messages = 
				KafkaUtils.createDirectStream(jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
						);
		JavaPairDStream<String, String> messages1=messages.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
				// TODO replace 'null' with key-value pair as tuple2
			//System.out.println("1. "+record.key()+record.value());
				return new Tuple2<String, String>(record.key(),record.value());
			}
		}); 
		JavaDStream<String> json = messages1.map(
				new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 42l;
					@Override
					public String call(Tuple2<String, String> message) {
						//System.out.println("2. "+message._2);
						String s=new String(message._2);
						//System.out.println("2."+s);
						return s;
					}
				}
				);
		JavaPairDStream<Long, String> tweets = json.mapToPair(twitterFilterFunction1());

		JavaPairDStream<Long, String> filtered = tweets.filter(
				new Function<Tuple2<Long, String>, Boolean>() {
					private static final long serialVersionUID = 42l;
					@Override
					public Boolean call(Tuple2<Long, String> tweet) {
						System.out.println("4. returning bool");
						return tweet != null;
					}
				}
				);
		JavaDStream<Tuple2<Long, String>> tweetsFiltered = filtered.map(textFilter());
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets =
				tweetsFiltered.mapToPair(positiveScoreFunction());
		
		//tweetsFiltered.print();

		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
				tweetsFiltered.mapToPair(negativeScoreFunction());
		
		//positiveTweets.print();
		//negativeTweets.print();

		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined =
				positiveTweets.join(negativeTweets);
		
		//joined.print(10);
		JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets =
				joined.map(new Function<Tuple2<Tuple2<Long, String>,
						Tuple2<Float, Float>>,
						Tuple4<Long, String, Float, Float>>() {
					private static final long serialVersionUID = 42l;
					@Override
					public Tuple4<Long, String, Float, Float> call(
							Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet)
							{
						return new Tuple4<Long, String, Float, Float>(
								tweet._1()._1(),
								tweet._1()._2(),
								tweet._2()._1(),
								tweet._2()._2());
							}
				});
		
		
		//System.out.println( "tweets joined");
		
		//System.out.println("scored tweets");
		//scoredTweets.print();
		
		JavaDStream<Tuple5<Long, String, Float, Float, String>> result =
	            scoredTweets.map(new ScoreTweetsFunction());
		
		result.print();
		result.foreachRDD(new VoidFunction2<JavaRDD<Tuple5<Long, String, Float, Float, String>>,Time>()
		{
			private static final long serialVersionUID = 42l;

			@Override
			public void call(
					JavaRDD<Tuple5<Long, String, Float, Float, String>> rdd,Time time)
			{
				if (rdd.count() > 0){ //return null;
					System.out.println("result save");
				String path = "/user/user01/" +
						"_" +
						time.milliseconds();
				rdd.saveAsTextFile(path);
				}
			//	return null;
			}
		});
		
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void loadstopWords()
	{
		
		BufferedReader rd = null;
		try
		{
			System.out.println("in load stop");
			rd = new BufferedReader(
					new InputStreamReader(
							SentimentAnalysis.class.getResourceAsStream("/stop-words.txt")));
			String line;
			while ((line = rd.readLine()) != null){
		//		System.out.println("loading....");
				stopWords.add(line);
			}
		}
		catch (IOException ex)
		{
			System.out.println(ex);
		}
	}

	private static void positiveWords()
	{
		BufferedReader rd = null;
		try
		{
			System.out.println("In positive");
			rd = new BufferedReader(
					new InputStreamReader(
							SentimentAnalysis.class.getResourceAsStream("/pos-words.txt")));
			String line;
			while ((line = rd.readLine()) != null){
		//	System.out.println("positive loading");
				posWords.add(line);
			}
		}
		catch (IOException ex)
		{
			System.out.println(ex);
		}
	}
	private static void negativeWords()
	{
		BufferedReader rd = null;
		try
		{
			System.out.println("in negative");

			rd = new BufferedReader(
					new InputStreamReader(
							SentimentAnalysis.class.getResourceAsStream("/neg-words.txt")));
			String line;
			while ((line = rd.readLine()) != null){
		//		System.out.println("neagtive loading");
				negWords.add(line);
			}
		}
		catch (IOException ex)
		{
			System.out.println(ex);
		}
	}
	private static PairFunction<String, Long, String> twitterFilterFunction() {

		return new PairFunction<String, Long, String>() {
			private static final long serialVersionUID = 1L;
			private final ObjectMapper mapper = new ObjectMapper();
			@Override
			public Tuple2<Long, String> call(String tweet){
				try{
				JsonNode root = mapper.readValue(tweet, JsonNode.class);
				long id;
				String text;
				if (root.get("lang") != null &&
						"en".equals(root.get("lang").textValue()))
				{
					if (root.get("id") != null && root.get("text") != null)
					{
						id = root.get("id").longValue();
						text = root.get("text").textValue().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();;
						return new Tuple2<Long, String>(id, text);
					}
					return null;
				}
				
			}catch(Exception e){
				System.out.println(e);
			}
				return null;
			}
		};
	}
	private static PairFunction<String, Long, String> twitterFilterFunction1() {

		return new PairFunction<String, Long, String>() {
			private static final long serialVersionUID = 1L;
			//private final ObjectMapper mapper = new ObjectMapper();
			@Override
			public Tuple2<Long, String> call(String tweet){
				try{
				//JsonNode root = mapper.readValue(tweet, JsonNode.class);
				//	System.out.println("3. am in");
					int maximum=99999;
				long id=tweet.length();//1 + (int)(Math.random() * maximum); ;
				String text1;
				//if (root.get("lang") != null &&
				//		"en".equals(root.get("lang").textValue()))
				//{
					//if (root.get("id") != null && root.get("text") != null)
					if(tweet!=null)
					{
						//id = root.get("id").longValue();
						text1 = tweet.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
					//	System.out.println("3. text");
						return new Tuple2<Long, String>(id, text1);
					}
					return null;
				}
				catch(Exception e){
				System.out.println(e);
			}
				return null;
			}
		};
	}
	private static Function<Tuple2<Long, String>, Tuple2<Long, String>> textFilter() {
		// TODO Auto-generated method stub
		return new Function<Tuple2<Long, String>, Tuple2<Long, String>>()
		{
		    private static final long serialVersionUID = 42l;

		    @Override
		    public Tuple2<Long, String> call(Tuple2<Long, String> tweet)
		    {
		        String text = tweet._2();
		    //    System.out.println("Textfilter");
		       
		        for (String word : stopWords)
		        {
		            text = text.replaceAll("\\b" + word + "\\b", "");
		        }
		        return new Tuple2<Long, String>(tweet._1(), text);
		    }
		};
	}
	private static PairFunction<Tuple2<Long, String>, Tuple2<Long, String>, Float> positiveScoreFunction() {
		// TODO Auto-generated method stub
		return new PairFunction<Tuple2<Long, String>,
				Tuple2<Long, String>, Float>()
				{
			final long serialVersionUID = 42l;

			@Override
			public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> tweet)
			{
				String text = tweet._2();
				System.out.println("calculating positive score");

				String[] words = text.split(" ");
				int numWords = words.length;
				int numPosWords = 0;
				for (String word : words)
				{
					if (posWords.contains(word))
						numPosWords++;
				}
				return new Tuple2<Tuple2<Long, String>, Float>(
						new Tuple2<Long, String>(tweet._1(), tweet._2()),
						(float) numPosWords / numWords
						);
			}
				};
	}
	
	private static PairFunction<Tuple2<Long, String>, Tuple2<Long, String>, Float> negativeScoreFunction() {
		return new PairFunction<Tuple2<Long, String>,
				Tuple2<Long, String>, Float>()
				{
			final long serialVersionUID = 42l;

			@Override
			public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> tweet)
			{
				String text = tweet._2();
				System.out.println("Calculating neg");
				String[] words = text.split(" ");
				int numWords = words.length;
				int numPosWords = 0;
				for (String word : words)
				{
					if (negWords.contains(word))
						numPosWords++;
				}
				return new Tuple2<Tuple2<Long, String>, Float>(
						new Tuple2<Long, String>(tweet._1(), tweet._2()),
						(float) numPosWords / numWords
						);
			}
				};
	}
/*	private static Function<Tuple4<Long, String, Float, Float>, Tuple5<Long, String, Float, Float, String>> scoreTweetsFunction() {
		// TODO Auto-generated method stub
		return new Function<Tuple4<Long, String, Float, Float>,
				Tuple5<Long, String, Float, Float, String>>()
				{
			
			private static final long serialVersionUID = 42l;

			@Override
			public Tuple5<Long, String, Float, Float, String> call(
					Tuple4<Long, String, Float, Float> tweet)
					{
				System.out.println("score1");
				String score;
				if (tweet._3() >= tweet._4())
					score = "positive";
				else
					score = "negative";
				System.out.println("score2");
				return new Tuple5<Long, String, Float, Float, String>(
						tweet._1(),
						tweet._2(),
						tweet._3(),
						tweet._4(),
						score);
				//System.out.println("score3");
					}
				};
	}*/
}
