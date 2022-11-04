import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetWriter;
import tech.tablesaw.api.ColumnType;

import tech.tablesaw.api.Table;
import tech.tablesaw.io.json.JsonReadOptions;
import tech.tablesaw.io.json.JsonReadOptions.Builder;

import static tech.tablesaw.aggregate.AggregateFunctions.firstString;

public class Parse_TableSaw {

	public static void main(String[] args) {
		int[] counts = new int[] { 100 };
		for (int count : counts) {
			long[] times = new long[3];
			System.out.printf("Running for %d\n", count);
			Path source = Paths.get("./data/connectdata-day=2022-09-19_device=s_96_2.json.gz");
			Path target = Paths.get("./data/connectdata-day=2022-09-19_device=s_96_2.json");
			String finalJSON = "";
			long startTime = System.nanoTime();
			try {
				Parse_TableSaw.decompressGzip(source, target);
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				finalJSON = Parse_TableSaw.formatJSON(target, count);
			} catch (IOException e) {
				e.printStackTrace();
			}
			times[0] = System.nanoTime() - startTime;

			startTime = System.nanoTime();
			Table table = Parse_TableSaw.ParseJSON(finalJSON);
			times[1] = System.nanoTime() - startTime;

			startTime = System.nanoTime();
			table = Parse_TableSaw.pivot(table);
			times[2] = System.nanoTime() - startTime;

			for (long time : times) {
				System.out.println(time / 1000000000.0);
			}

			// Parse_TableSaw.convertToParquet(table);
			File file = new File("./data/connectdata-day=2022-09-19_device=s_96_2.json");
			file.delete();
			file = new File("./data/connectdata-day=2022-09-19_device=s_96_2_modified.json");
			file.delete();
		}
	}

	public static void decompressGzip(Path source, Path target) throws IOException {
		try (GZIPInputStream gis = new GZIPInputStream(
				new FileInputStream(source.toFile()));
				FileOutputStream fos = new FileOutputStream(target.toFile())) {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
		}
	}

	public static String formatJSON(Path source, int count) throws IOException {
		BufferedReader reader;
		String dest = source.toString();
		try {
			reader = new BufferedReader(new FileReader(
					source.toString()));
			String line = reader.readLine(), prev = null;
			dest = dest.substring(0, dest.length() - 5) + "_modified.json";
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(dest));
			bw.write("[");
			while (line != null) {
				if (prev != null) {
					bw.write(prev + ",\n");
					count--;
				}
				prev = line;
				if (count == 1)
					break;
				line = reader.readLine();
			}

			bw.write(prev + "]");
			reader.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dest;
	}

	public static Table ParseJSON(String path) {
		ColumnType types[] = new ColumnType[100];
		Arrays.fill(types, ColumnType.STRING);
		Builder builder = JsonReadOptions.builderFromFile(path).columnTypes(types);
		Table table = Table.read().usingOptions(builder);
		return table;
	}

	public static Table pivot(Table table) {
		Table table1 = table.pivot("timestamp", "data_item_name", "value", firstString);
		return table1;
	}

	public static void convertToParquet(Table table) {
		File parquet = new File("./final.parquet");
		new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(parquet).build());
	}

}