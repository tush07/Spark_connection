package sparkHiveTrial.HiveTrial;

import java.io.File;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
//1st argument is the metastore uri 
//2nd argument is the name of the table
//3rd argument is the path for the file which has data that has to be loaded 
//4th argument is the path for the output directory
public class App {
	public static void main(String[] args) {
		String hiveMetastoreUris = args[0];
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("spark.sql.warehouse.dir", warehouseLocation).config("hive.metastore.uris", hiveMetastoreUris)
				.enableHiveSupport().getOrCreate();

		String tableName = args[1];
		String pathForDataFile = args[2];
		String outputDirectoryPath = args[3];

		String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (key INT, value STRING) USING hive";
		String loadDataIntoTableQuery = "LOAD DATA LOCAL INPATH '" + pathForDataFile + "' INTO TABLE " + tableName;
		String getDataFromTableQuery = " INSERT OVERWRITE LOCAL DIRECTORY '" + outputDirectoryPath + "' SELECT * FROM "
				+ tableName;
		String selectAllFromTable = "SELECT * FROM " + tableName;

		// Queries are expressed in HiveQL
		spark.sql(createTableQuery);
		spark.sql(loadDataIntoTableQuery);
		spark.sql(getDataFromTableQuery);
		spark.sql(selectAllFromTable).show();
		// Aggregation queries are also supported.
		spark.sql("SELECT COUNT(*) FROM " + tableName).show();
		spark.close();

	}
}
