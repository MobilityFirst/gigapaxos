package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Properties;
import java.util.Set;

public class BookScannerDBApp implements Replicable {

    private final String checkpointFilename;
    private final String restoreFilename;
    protected Connection connection = null;
    private String tableName;
    private int currRowId = -1;


    public BookScannerDBApp(String[] args) {
        super();
        Properties properties = PaxosConfig.getAsProperties();
        this.checkpointFilename = properties.getProperty("CURRENT_STATE_DIR") + "/checkpoint_" + args[0] + ".sql";
        this.restoreFilename = properties.getProperty("CURRENT_STATE_DIR") + "/restore_" + args[0] + ".sql";
        try {
            this.tableName = "book_" + args[0];
            connection = DriverManager
                    .getConnection(properties.getProperty("POSTGRES_CONNECTION_URL"),
                            properties.getProperty("POSTGRES_USERNAME"), properties.getProperty("POSTGRES_PASSWORD"));
            prepareDatabase();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareDatabase() throws SQLException {
        Statement stmt = connection.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS " + this.tableName + " " +
                "(ID INT PRIMARY KEY NOT NULL," +
                " PAGE_NO INT, " +
                " PAGE_TEXT TEXT, " +
                " DATETIME_ADDED TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        stmt.executeUpdate(sql);
        stmt.close();
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public boolean execute(Request request) {
        if (request instanceof RequestPacket) {
            String requestValue = ((RequestPacket) request).requestValue;
            try {
                if (currRowId == -1) {
                    ResultSet rs = connection.createStatement()
                            .executeQuery(String.format("SELECT * FROM %s order by id desc limit 1;", tableName));
                    currRowId = 0;
                    while (rs.next()) {
                        currRowId = rs.getInt("id");
                    }
                }

                JSONObject jsonObject = new JSONObject(requestValue);
                String pageText = jsonObject.getString("page_text");
                int pageNo = jsonObject.getInt("page_no");
                this.currRowId += 1;
                String query = String.format("INSERT INTO %s (ID, PAGE_NO, PAGE_TEXT) values (%d, %d, '%s');",
                        this.tableName, this.currRowId, pageNo, pageText);
                connection.createStatement().executeUpdate(query);
                ((RequestPacket) request).setResponse("Saved to DB successfully");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else
            System.err.println("Unknown request type: " + request.getRequestType());
        return true;
    }

    @Override
    public String checkpoint(String name) {
        Path checkpointPath = Paths.get(this.checkpointFilename);
        try {
            if (!Files.exists(checkpointPath)) {
                Files.createFile(checkpointPath);
            }
            Files.writeString(checkpointPath, "", StandardOpenOption.TRUNCATE_EXISTING);
            ResultSet resultSet = connection.createStatement()
                    .executeQuery(String.format("select * from %s order by id;", tableName));
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                int pageNo = resultSet.getInt("page_no");
                String pageText = resultSet.getString("page_text");
                String timestamp = resultSet.getString("datetime_added");
                Files.writeString(checkpointPath, String.format("INSERT INTO %s (id, page_no, page_text, datetime_added) " +
                        "values (%d, %d, E'%s', '%s');", this.tableName, id, pageNo, pageText.replace("\n", "\\n"), timestamp) + System.lineSeparator(), StandardOpenOption.APPEND);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return LargeCheckpointer.createCheckpointHandle(this.checkpointFilename);
    }

    @Override
    public boolean restore(String name, String state) {
        if (LargeCheckpointer.isCheckpointHandle(state)) {
            try {
                if (Files.notExists(Paths.get(this.restoreFilename))) {
                    Files.createFile(Paths.get(this.restoreFilename));
                }
                LargeCheckpointer.restoreCheckpointHandle(state, this.restoreFilename);
                connection.createStatement().executeUpdate(String.format("TRUNCATE %s;", this.tableName));

                File file = new File(this.restoreFilename);
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null) {
                    connection.createStatement().executeUpdate(line);
                }
                fr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }
}
