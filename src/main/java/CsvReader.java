import com.opencsv.CSVReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public class CsvReader {

    private CSVReader csvReader;
    private List<String> headers;
    private String[] rowValues;
    private HashMap<String, String> map = new HashMap<>();

    // METRICS
    private long numberOfRows = 0;
    private long numberOfSkippedEntries = 0;


    CsvReader(Reader input, List<String> requiredHeaders, String pk, String validatingKey) throws IOException {
        csvReader = new CSVReader(input);
        String[] line = csvReader.readNext();

        if (line == null)
            throw new RuntimeException("No rows in CSV file");

        headers = Stream.of(line).map(String::trim).collect(Collectors.toList());

        for (String requiredHeader : requiredHeaders) {
            if (!headers.contains(requiredHeader))
                throw new RuntimeException("CSV does not contain header " + requiredHeader);
        }

        populateMap(pk, validatingKey);

        System.out.println("TOTAL NUMBER OF LINES: " + numberOfRows);
        System.out.println("TOTAL NUMBER OF SKIPPED LINES: " + numberOfSkippedEntries);
        System.out.println("TOTAL NUMBER OF ENTRIES PUT INTO THE MAP: " + (numberOfRows - numberOfSkippedEntries));

        Set<String> keyset = map.keySet();
        System.out.println("TOTAL NUMBER OF UNIQUE KEYS: " + keyset.size());

        for (String key : map.keySet()) {
            String value = map.get(key);
            if (!StringUtils.isNumeric(value)) {
                System.out.println("VALUE NOT NUMERIC: " + keyset.size());
            }
        }
    }

    HashMap<String, String> getMap() {
        return map;
    }

    private void populateMap(String pk, String validatingKey) throws IOException {
        rowValues = csvReader.readNext();

        while (rowValues != null) {
            numberOfRows++;
            if (currentRowShouldBeSkipped()) {
                numberOfSkippedEntries++;
            } else {
                if (rowValues.length != headers.size())
                    throw new RuntimeException("Invalid row length: " + Arrays.toString(rowValues));

                map.put(valueOf(pk), valueOf(validatingKey));
            }
            rowValues = csvReader.readNext();
        }
    }

    private String valueOf(String headerName) {
        int i = headers.indexOf(headerName);
        return i >= 0 ? rowValues[i].trim() : null;
    }

    // set condition for which you want a row to be skipped
    private boolean currentRowShouldBeSkipped() {
        return false;
    }

    @Override
    public String toString() {
        return headers.toString();
    }
}
