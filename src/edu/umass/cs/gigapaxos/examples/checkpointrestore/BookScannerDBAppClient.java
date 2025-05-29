package edu.umass.cs.gigapaxos.examples.checkpointrestore;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BookScannerDBAppClient extends PaxosClientAsync {

    private static String bookPath;
    public BookScannerDBAppClient() throws IOException {
        super();
        bookPath = PaxosConfig.getAsProperties().getProperty("bookPath");
    }


    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        BookScannerDBAppClient client = new BookScannerDBAppClient();
        Stream<String> bookLines = Files.lines(Paths.get(bookPath));
        AtomicInteger counter = new AtomicInteger();
        List<String> groupedPages = bookLines.collect(Collectors.groupingBy(a -> counter.getAndIncrement() / 20))
                .values().stream().map(x -> String.join("\n", x)).toList();

        for (int i = 1; i <= groupedPages.size(); i++) {
            System.out.print("Request #" + i);
            JSONObject jsonObject = new JSONObject(Map.of("page_no", i, "page_text", groupedPages.get(i-1)));

            client.sendRequest(PaxosConfig.getDefaultServiceName(), jsonObject.toString(), new RequestCallback() {
                final long createTime = System.currentTimeMillis();

                @Override
                public void handleResponse(Request response) {
                    System.out
                            .println(": Response for request ["
                                    + "] = "
                                    + ((RequestPacket) response).getResponseValue()
                                    + " received in "
                                    + (System.currentTimeMillis() - createTime)
                                    + "ms");

                }
            });
            Thread.sleep(100);
        }
        client.close();
    }
}
