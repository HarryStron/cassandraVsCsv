import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.HashMap;

class CassandraVerifier {
    private Cluster cluster;
    private Session session;

    void connect(final String node, String username, String password, String clusterName) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withCredentials(username, password)
                .withClusterName(clusterName)
                .build();

        final Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (final Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    void close() {
        cluster.close();
    }

    void validateEntriesFromMap(HashMap<String, String> map, String pk, String validatingKey, String tableName) {
        long valueFound = 0;
        PreparedStatement selectByPrimaryKey = session.prepare(
                "SELECT " + validatingKey + " FROM " + tableName + " WHERE " + pk + " = ?"
        );

        for (String key: map.keySet()) {
            Row row = session.execute(selectByPrimaryKey.bind(key)).one();
            if (row == null)
                System.out.println("Entry missing for: " +  key + " : " + map.get(key));
            else if (!row.getString(1).equals(map.get(key)))
                System.out.println("DB row does not match the CSV entry for key: " + key + "\n"
                                    + "DB: " + row.getString(1) + " --- CSV: " + map.get(key));
            else
                valueFound++;
        }
        System.out.println("TOTAL NUMBER OF ENTRIES VERIFIED: " + valueFound);
    }
}
