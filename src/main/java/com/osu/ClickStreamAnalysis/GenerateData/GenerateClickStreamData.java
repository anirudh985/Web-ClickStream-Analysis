package com.osu.ClickStreamAnalysis.GenerateData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.PrimitiveIterator.OfDouble;
import java.util.PrimitiveIterator.OfLong;
import java.util.Random;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class GenerateClickStreamData {
	public static void main(String[] args) throws IOException {
		long numberOfUsers = Long.parseLong(args[0]);
		long numberOfPages = Long.parseLong(args[1]);
		long numberOfCategories = Long.parseLong(args[2]);
		long timeRange = Long.parseLong(args[3]);

		long numberOfClickStreams = numberOfUsers * 1000;

		String outputPath = args[4];

		Random randomUserGenerator = new Random(1234567);
		Random randomPageGenerator = new Random(3456712);
		Random randomCategoryGenerator = new Random(5671234);
		Random randomTimestampGenerator = new Random(9871234);
		
		OfLong randomUserStream = randomUserGenerator.longs(1, numberOfUsers + 1).iterator();
		OfLong randomPageStream = randomPageGenerator.longs(1, numberOfPages + 1).iterator();
		OfLong randomCategoryStream = randomCategoryGenerator.longs(1, numberOfCategories + 1).iterator();
		OfDouble randomTimestampStream = randomTimestampGenerator.doubles(1, timeRange + 1).iterator();
		

		File outputFile = new File(outputPath);
		FileWriter fw = new FileWriter(outputFile, false);
		PrintWriter out = new PrintWriter(fw);

		for (long i = 0; i < numberOfClickStreams; i++) {
			out.println(
					String.valueOf(randomUserStream.next()) + Constants.SPACE + String.valueOf(randomPageStream.next())
							+ Constants.SPACE + String.valueOf(randomCategoryStream.next()) + Constants.SPACE
							+ String.valueOf(randomTimestampStream.next()));
		}
		fw.close();

	}
}
