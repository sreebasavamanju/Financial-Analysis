package com.learning.java.Financial_Analysis_Java;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class NiftyAnalyzer {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Nifty50-Analyzer").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> df = spark.read().json(args[0]);
		df = df.withColumn("last_close", functions.round(functions.col("last_closeRaw").cast(DataTypes.DoubleType), 2))
				.withColumn("last_max", functions.round(functions.col("last_maxRaw").cast(DataTypes.DoubleType), 2))
				.withColumn("last_min", functions.round(functions.col("last_minRaw").cast(DataTypes.DoubleType), 2))
				.withColumn("last_open", functions.round(functions.col("last_openRaw").cast(DataTypes.DoubleType), 2))
				.withColumn("date", functions.from_unixtime(functions.col("rowDateRaw")).cast(DataTypes.DateType))
				.withColumn("change_percent",
						functions.round(functions.col("change_percentRaw").cast(DataTypes.DoubleType), 2));
		df = df.drop("last_maxRaw", "last_maxRaw", "last_minRaw", "last_openRaw", "rowDateRaw", "change_percentRaw",
				"last_closeRaw");
		Dataset<Row> avgChange = df.withColumn("Day", functions.dayofmonth(functions.col("date"))).groupBy("day")
				.agg(functions.round(functions.avg("change_percent"), 4).as("avg_change_percent"))
				.orderBy(functions.col("day").asc());
		avgChange.show();

		System.out.println("Last ThursDay Dates");
		List<LocalDate> lastThursdayDates = getLastThursdayDates(2001, 2024);
		df.where(functions.col("date").isInCollection(lastThursdayDates))
				.agg(functions.round(functions.avg(functions.col("change_percent")), 2)).show();

	}

	private static List<LocalDate> getLastThursdayDates(int startYear, int endYear) {
		List<LocalDate> dates = new ArrayList<LocalDate>();
		for (int year = startYear; year <= endYear; year++) {
			for (int month = 1; month <= 12; month++) {
				dates.add(LocalDate.of(year, month, 1).with(TemporalAdjusters.lastInMonth(DayOfWeek.THURSDAY)));
			}
		}
		return dates;
	}
}
