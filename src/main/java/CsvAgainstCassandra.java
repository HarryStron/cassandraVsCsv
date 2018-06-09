import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class CsvAgainstCassandra {
    private static final String PATH_OF_CSV_FILE = "/path/of/file.csv";
    private static final String TABLE_NAME = "cassandraTableName";
    private static final String CLUSTER_NAME = "cassandraCluster";
    private static final String PASSWORD = "cassandraPass";
    private static final String USERNAME = "cassandraUser";
    private static final String NODE = "cassandraNode";
    private static final String PRIMARY_KEY_HEADER = "primaryKeyHeader";
    private static final String HEADER_TO_TEST = "headerWeWantToTestAgainst";
    private static final ArrayList<String> REQUIRED_HEADERS = new ArrayList<String>() {{
        add("header1");
        add("header2");
        add("header3");
    }};

    public static void main(String[] args) throws IOException {
        File csvFile = new File(PATH_OF_CSV_FILE);
        InputStream inputStream = new FileInputStream(csvFile);
        CsvReader csvReader = new CsvReader(new InputStreamReader(inputStream, "UTF-8"),
                                            REQUIRED_HEADERS, PRIMARY_KEY_HEADER, HEADER_TO_TEST);
        HashMap<String, String> map = csvReader.getMap();

        CassandraVerifier client = new CassandraVerifier();
        client.connect(NODE, USERNAME, PASSWORD, CLUSTER_NAME);
        client.validateEntriesFromMap(map, PRIMARY_KEY_HEADER, HEADER_TO_TEST, TABLE_NAME);

        client.close();
    }
}
