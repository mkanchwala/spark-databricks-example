package com.mkanchwala.databricks.launcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.Metadata;

import com.mkanchwala.databricks.generator.BookGenerator;
import com.mkanchwala.databricks.model.Book;

public class XMLParserLauncher {

	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println(
					"Incomplete parameters. Please provide IsLocalMode(Y/N), BookObjects, Partitions, SaveLocation");
		}

		String isLocalMode = args[0];
		Integer bookCounts = Integer.parseInt(args[1]);
		Integer partitions = Integer.parseInt(args[2]);
		String saveLocation = args[3];

		System.out.println(
				"XMLWriter <" + isLocalMode + "> <" + bookCounts + "> <" + partitions + "> <" + saveLocation + ">");

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("XMLWriter");
		if (isLocalMode.equalsIgnoreCase("Y")) {
			sparkConf.setMaster("local[4]");
		}

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlCon = new SQLContext(javaSparkContext);

		final JavaRDD<Book> parallelize = javaSparkContext.parallelize(BookGenerator.generate(bookCounts));

		final JavaRDD<Row> map = parallelize.map(book -> RowFactory.create(book.getName(), book.getTest()));

		List<StructField> fields = new ArrayList<>();
		fields.add(new StructField("Name", DataTypes.StringType, true, Metadata.empty()));
		fields.add(new StructField("Test", DataTypes.StringType, true, Metadata.empty()));
		final StructType structType = DataTypes.createStructType(fields);

		final Dataset<Row> dataFrame = sqlCon.createDataFrame(map, structType);

		dataFrame.repartition(partitions).write().format("com.databricks.spark.xml").mode(SaveMode.Overwrite)
				.option("rootTag", "n:Brands").option("rowTag", "n:Brand").save(saveLocation);

	}

}
