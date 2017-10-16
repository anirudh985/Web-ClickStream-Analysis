package com.osu.ClickStreamAnalysis.GenerateData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.PrimitiveIterator.OfDouble;
import java.util.PrimitiveIterator.OfLong;
import java.util.Random;
import java.util.stream.DoubleStream;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class GenerateClickStreamSmallData {
	public static void main(String[] args) throws IOException {

		String outputPath = args[0];
		
		long[] users = new long[] {1,1,1,1,2,2,2,2,1,1,1};
		long[] pages = new long[] {1234,421,667,9,1234,56789,12345,2,23,45,7689};
		long[] categories = new long[] {1,1,2,3,1,2,6,3,1,2,3};
		double[] timestamp = new double[] {1.01,2.01,3.02,4.02,1.05,1.51,1.59,2.01,8.33,9.35,9.59};

		File outputFile = new File(outputPath);
		FileWriter fw = new FileWriter(outputFile, false);
		PrintWriter out = new PrintWriter(fw);

		for (int i = 0; i < 11; i++) {
			out.println(String.valueOf(users[i]) + Constants.SPACE + String.valueOf(pages[i]) + Constants.SPACE
					+ String.valueOf(categories[i]) + Constants.SPACE + String.valueOf(timestamp[i]));
		}
		fw.close();

	}
}
