package TwitterAnalysis.Twitter;

import java.io.IOException;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaDecisionTreeClassification {
	public static void main(String[] args) {
		System.out.println(args);
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("JavaDecisionTreeClassificationExample")
	      .getOrCreate();
	    StructType schema =  new StructType(new StructField[]{
	    		new StructField("id", DataTypes.LongType, false, Metadata.empty()),
	    		new StructField("text", DataTypes.StringType, false, Metadata.empty()),
	    		new StructField("ratio_pos_words", DataTypes.DoubleType, false, Metadata.empty()),
	    		new StructField("ratio_neg_words", DataTypes.DoubleType, false, Metadata.empty()),
	    		new StructField("no_words", DataTypes.IntegerType, false, Metadata.empty()),
	    		  new StructField("no_chars", DataTypes.IntegerType, false, Metadata.empty()),
		    		new StructField("label", DataTypes.StringType, false, Metadata.empty())
	    });

	    Dataset<Row> data = spark.read()
	            .format("org.databricks.spark.csv")
	            .schema(schema)
	            //.option("header", "true") //reading the headers
	            .csv(args[1]);
	    data.show(10);
	    System.out.println("************");
	    

	    StringIndexerModel labelIndexer = new StringIndexer()
	      .setInputCol("label")
	      .setOutputCol("indexedLabel")
	      .fit(data);
	    StringIndexerModel vectorfeatureIndexer = new StringIndexer()
	      .setInputCol("ratio_pos_words")
	      .setOutputCol("vectorratio_pos_words")
	      .fit(data);
	    StringIndexerModel vectorfeatureIndexer1 = new StringIndexer()
	      .setInputCol("ratio_neg_words")
	      .setOutputCol("vectorratio_neg_words")
	      .fit(data);
	    StringIndexerModel vectorfeatureIndexer2 = new StringIndexer()
	      .setInputCol("no_words")
	      .setOutputCol("vectorno_words")
	      .fit(data);
	    StringIndexerModel vectorfeatureIndexer3 = new StringIndexer()
	      .setInputCol("no_chars")
	      .setOutputCol("vectorno_chars")
	      .fit(data);
	    
	    
	    VectorAssembler vectAssembler=new VectorAssembler().setInputCols(new String[]{"vectorratio_pos_words"
	    		,"vectorratio_neg_words","vectorno_words","vectorno_chars"})
	    		.setOutputCol("indexedFeatures");
	    // Automatically identify categorical features, and index them.
	   /* VectorIndexerModel featureIndexer = new VectorIndexer()
	      .setInputCol("vectorfeature1")
	      .setOutputCol("indexedFeatures")
	      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
	      .fit(data);*/

	    // Split the data into training and test sets (30% held out for testing).
	    Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
	    Dataset<Row> trainingData = splits[0];
	    Dataset<Row> testData = splits[1];
	    Column id= trainingData.col("id");
	    Column id1=testData.col("id");
	    trainingData=trainingData.drop(trainingData.col("id"));
	    testData=testData.drop(testData.col("id"));
	    trainingData=trainingData.drop(trainingData.col("text"));
	    testData=testData.drop(testData.col("text"));
	    
	    // Train a DecisionTree model.
	    DecisionTreeClassifier dt = new DecisionTreeClassifier()
	      .setLabelCol("indexedLabel")
	      .setFeaturesCol("indexedFeatures");

	    // Convert indexed labels back to original labels.
	    IndexToString labelConverter = new IndexToString()
	      .setInputCol("prediction")
	      .setOutputCol("predictedLabel")
	      .setLabels(labelIndexer.labels());

	    // Chain indexers and tree in a Pipeline.
	    Pipeline pipeline = new Pipeline()
	      .setStages(new PipelineStage[]{labelIndexer,vectorfeatureIndexer,vectorfeatureIndexer1,
	    		  vectorfeatureIndexer2,vectorfeatureIndexer3,vectAssembler,  dt, labelConverter});
	    
	    // Train model. This also runs the indexers.
	    PipelineModel model = pipeline.fit(trainingData);
	    model.extractParamMap();
	    // Make predictions.
	    Dataset<Row> predictions = model.transform(testData);
	    predictions.show();

	    // Select example rows to display.
	    		predictions.select("predictedLabel", "label").show(5);

	    // Select (prediction, true label) and compute test error.
	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
	      .setLabelCol("indexedLabel")
	      .setPredictionCol("prediction")
	      .setMetricName("accuracy");
	    double accuracy = evaluator.evaluate(predictions);
	    System.out.println("Test Error = " + (1.0 - accuracy));

	    DecisionTreeClassificationModel treeModel =
	      (DecisionTreeClassificationModel) (model.stages()[6]);
	    System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
	    try {
	    	System.out.println(args[0]+"DT.csv");
			treeModel.save(args[0]+"DT.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    // $example off$
	    DecisionTreeClassificationModel treeModel1= DecisionTreeClassificationModel.read().load(args[0]+"DT.csv");
	    treeModel1.explainParams();
	    spark.stop();
	  }
	}
