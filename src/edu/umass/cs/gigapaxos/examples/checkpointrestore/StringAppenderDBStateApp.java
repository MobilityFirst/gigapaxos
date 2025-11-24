package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;

public class StringAppenderDBStateApp implements Replicable {

    protected Connection connection = null;
    protected String tableName = "";

    public StringAppenderDBStateApp(String[] args) {
        super();
        Properties properties = PaxosConfig.getAsProperties();
        try {
            this.tableName = "text_" + args[0];
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
                " TYPE TEXT NOT NULL, " +
                " VALUE TEXT, " +
                " FINAL_RESULT TEXT)";
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
                ResultSet rs = connection.createStatement()
                        .executeQuery(String.format("SELECT * FROM %s order by id desc limit 1;", tableName));
                int currRowId = 0;
                String currFinalText = "";
                while (rs.next()) {
                    currRowId = rs.getInt("id");
                    currFinalText = rs.getString("final_result");
                }
                JSONObject jsonObject = new JSONObject(requestValue);
                String typeOfReq = jsonObject.getString("type");
                String insertStatement = switch (typeOfReq) {
                    case "type" -> {
                        currFinalText += jsonObject.getString("value");
                        yield String.format("INSERT INTO %s (id, type, value, final_result) values (%d, 'type', '%s', '%s');",
                                tableName, (currRowId + 1), jsonObject.getString("value"), currFinalText);
                    }
                    case "backspace" -> {
                        currFinalText = currFinalText.substring(0, currFinalText.length() - 1);
                        yield String.format("INSERT INTO %s (id, type, final_result) values (%d, 'backspace', '%s');",
                                tableName, (currRowId + 1), currFinalText);
                    }
                    case "newline" -> {
                        currFinalText += System.lineSeparator();
                        yield String.format("INSERT INTO %s (id, type, final_result) values (%d, 'newline', '%s');",
                                tableName, (currRowId + 1), currFinalText);

                    }
                    case "cleartext" ->
                            String.format("INSERT INTO %s (id, type, final_result) values (%d, 'cleartext', '');",
                                    tableName, (currRowId + 1));
                    default -> "";
                };
                connection.createStatement().executeUpdate(insertStatement);
                ((RequestPacket) request).setResponse("Current text = " + currFinalText);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else System.err.println("Unknown request type: " + request.getRequestType());
        return true;
    }

    @Override
    public String checkpoint(String name) {
        String insertStatement = "";
        try {
            ResultSet resultSet = connection.createStatement()
                    .executeQuery(String.format("select * from %s order by id desc limit 1;", tableName));
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String type = resultSet.getString("type");
                Optional<String> value = Optional.ofNullable(resultSet.getString("value"));
                String currFinalText = resultSet.getString("final_result");
                insertStatement =
                        value.map(v -> String.format("INSERT INTO %s (id, type, value, final_result) " +
                                        "values (%d, '%s', '%s', '%s');", tableName, id, type, v, currFinalText))
                                .orElse(String.format("INSERT INTO %s (id, type, final_result) " +
                                        "values (%d, '%s', '%s');", tableName, id, type, currFinalText));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONObject(Map.of("tableName", this.tableName, "data", insertStatement)).toString();
    }

    @Override
    public boolean restore(String name, String state) {
        if (state == null || state.equals(Config.getGlobalString(PaxosConfig.PC
                .DEFAULT_NAME_INITIAL_STATE))) {
            return true;
        }
        try {
            JSONObject jsonObject = new JSONObject(state);
            String tableName = jsonObject.getString("tableName");
            String insertQuery = jsonObject.getString("data");
            this.tableName = tableName;
            connection.createStatement().executeUpdate(String.format("TRUNCATE %s;", tableName));
            connection.createStatement().executeUpdate(insertQuery);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
