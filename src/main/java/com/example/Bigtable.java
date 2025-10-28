// Important Imports
package com.example;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


// Using Google Cloud Bigtable to store and query weather data
public class Bigtable {    
    public final String projectId = "iitj-bigtable-project";    
    public final String instanceId = "iitj-bigtable-instance";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather";
    public BigtableDataClient btDataClient;
    public BigtableTableAdminClient btAdminClient;       
    
    // Function to connect to Bigtable
    public void bigTableConnect() throws IOException {
        System.out.println("**** Connecting to the BigTable ****");
        BigtableDataSettings dSettings =
            BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
        BigtableTableAdminSettings btadminSettings =
            BigtableTableAdminSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
        this.btDataClient = BigtableDataClient.create(dSettings);
        this.btAdminClient = BigtableTableAdminClient.create(btadminSettings);
        System.out.println("****Connection successful****");
    }

    // Function to run all operations
    public void run() throws Exception {
        bigTableConnect();
        deleteTable();
        createTable();
        load();
        // NOTE: The first time you run this, it will create and load the table.
        // After that, you can comment out deleteTable(), createTable(), and loadData()
        // to run only the queries.
        int temperature = query1();
        System.out.println("Query1_Result: Temperature At Vancouver On 2022-10-01 10:00: " + temperature + " degree farenheit ");
        int wind_speed = query2();
        System.out.println("Query2_Result: Highest Wind Speed In Portland In Sept 2022: " + wind_speed + " miles/h");
        ArrayList<Object[]> data = query3();
        System.out.println("Query3_Result: All Readings For SeaTac On October 2, 2022:");
        StringBuffer buf = new StringBuffer();
        buf.append("DATE       HOUR TEMP DEW HUMIDITY WINDSPEED PRESSURE\n");
        for (Object[] vals : data) {
            buf.append(String.format("%s %s   %s   %s  %s        %s         %s\n", vals));
        }
        System.out.println(buf.toString());
        temperature = query4();
        System.out.println("Query4_Result: Highest Temperature In Summer 2022: " + temperature + " degree farenheit ");
        close();
    }

    // Function to close the clients
    public void close() {
        System.out.println("****Closing clients****");
        if (btDataClient != null) {
            btDataClient.close();
        }
        if (btAdminClient != null) {
            btAdminClient.close();
        }
    }

    // Function to create the table
    public void createTable() {
        System.out.println("Creating the table..... " + tableId);
        if (!btAdminClient.exists(tableId)) {
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
            btAdminClient.createTable(createTableRequest);
            System.out.printf("Table %s created successfully%n", tableId);
        } else {
            System.out.printf("Table %s already exists.%n", tableId);
        }
    }

    // Function to load data into the table
    public void load() throws Exception {
        String dataPath = "data/";
        BulkMutation batch = BulkMutation.create(tableId);
        long mutationCountInCurrentBatch = 0;
        long totalMutationsSent = 0;
        final long BATCH_LIMIT = 85000; 

        String[] files = {"seatac.csv", "vancouver.csv", "portland.csv"};
        String[] stationIds = {"SEA", "YVR", "PDX"};

        try {
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i];
                String stationId = stationIds[i];
                System.out.println("Loading Data For " + stationId + " from " + fileName);

                File file = new File(dataPath + fileName);
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    String lastHour = "";
                    reader.readLine(); 

                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",");

                        if (values.length > 8) { 
                            String date = values[1];
                            String time = values[2];
                            String dateTime = date + " " + time;

                            if (dateTime.length() >= 13) {
                                String currentHour = dateTime.substring(0, 13);

                                if (!currentHour.equals(lastHour)) {
                                    lastHour = currentHour;
                                    String rowKey = stationId + "#" + currentHour.replace(" ", "-");

                                    RowMutationEntry mutationEntry = RowMutationEntry.create(rowKey)
                                        .setCell(COLUMN_FAMILY, "temperature", values[3])
                                        .setCell(COLUMN_FAMILY, "dewpoint", values[4])
                                        .setCell(COLUMN_FAMILY, "humidity", values[5])
                                        .setCell(COLUMN_FAMILY, "windspeed", values[6])
                                        .setCell(COLUMN_FAMILY, "pressure", values[8]);
                                    
                                    batch.add(mutationEntry);
                                    mutationCountInCurrentBatch += 5;

                                    if (mutationCountInCurrentBatch >= BATCH_LIMIT) {
                                        System.out.println("Sending batch of " + mutationCountInCurrentBatch + " mutations.");
                                        btDataClient.bulkMutateRows(batch);
                                        totalMutationsSent += mutationCountInCurrentBatch;
                                        
                                        batch = BulkMutation.create(tableId);
                                        mutationCountInCurrentBatch = 0;
                                    }
                                }
                            }
                        }
                    }
                }
            }            
            if (mutationCountInCurrentBatch > 0) {
                System.out.println("Sending final batch of " + mutationCountInCurrentBatch + " mutations.");
                btDataClient.bulkMutateRows(batch);
                totalMutationsSent += mutationCountInCurrentBatch;
            }
            System.out.println("Data loaded successfully. Total mutations sent: " + totalMutationsSent);
        } catch (IOException e) {
            throw new Exception("Error loading data file: " + e.getMessage(), e);
        }
    }

    // Query returns the temperature at Vancouver on October 1, 2022 at 10:00.
    public int query1() throws Exception {
        System.out.println("\n****Executing query1...");
        String rowKey = "YVR#2022-10-01-10";
        Row row = btDataClient.readRow(tableId, rowKey);
        if (row != null) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                return Integer.parseInt(cell.getValue().toStringUtf8());
            }
        }
        return -999; // Not found
    }

    // Query returns the highest wind speed recorded in Portland during September 2022.
    public int query2() throws Exception {
        System.out.println("****Executing query2...");
        int maxWindSpeed = 0;
        Query query = Query.create(tableId).prefix("PDX#2022-09");
        ServerStream<Row> rows = btDataClient.readRows(query);

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "windspeed")) {
                String windSpeedStr = cell.getValue().toStringUtf8();
                if (windSpeedStr != null && !windSpeedStr.isEmpty()) {
                    int currentWindSpeed = Integer.parseInt(windSpeedStr);
                    if (currentWindSpeed > maxWindSpeed) {
                        maxWindSpeed = currentWindSpeed;
                    }
                }
            }
        }
        return maxWindSpeed;
    }

    // Query returns all readings for SeaTac on October 2, 2022.
    public ArrayList<Object[]> query3() throws Exception {
        System.out.println("***Executing query #3...");
        ArrayList<Object[]> data = new ArrayList<>();
        Query query = Query.create(tableId).prefix("SEA#2022-10-02");
        ServerStream<Row> rows = btDataClient.readRows(query);

        for (Row row : rows) {
            String[] keyParts = row.getKey().toStringUtf8().split("#");
            String[] dateTimeParts = keyParts[1].split("-");
            String date = dateTimeParts[0] + "-" + dateTimeParts[1] + "-" + dateTimeParts[2];
            String hour = dateTimeParts[3];

            int temperature = Integer.parseInt(row.getCells(COLUMN_FAMILY, "temperature").get(0).getValue().toStringUtf8());
            int dewpoint = Integer.parseInt(row.getCells(COLUMN_FAMILY, "dewpoint").get(0).getValue().toStringUtf8());
            String humidity = row.getCells(COLUMN_FAMILY, "humidity").get(0).getValue().toStringUtf8();
            String windspeed = row.getCells(COLUMN_FAMILY, "windspeed").get(0).getValue().toStringUtf8();
            String pressure = row.getCells(COLUMN_FAMILY, "pressure").get(0).getValue().toStringUtf8();

            data.add(new Object[]{date, hour, temperature, dewpoint, humidity, windspeed, pressure});
        }
        return data;
    }

   // Query returns the highest temperature recorded across all three stations during the summer months (July and August) of 2022.
    public int query4() throws Exception {
        System.out.println("****Executing query #4...");
        int maxTemp = -100;
        String[] stations = {"SEA", "PDX", "YVR"};
        String[] summerMonths = {"#2022-07", "#2022-08"};

        for (String station : stations) {
            for (String month : summerMonths) {
                Query query = Query.create(tableId).prefix(station + month);
                ServerStream<Row> rows = btDataClient.readRows(query);

                for (Row row : rows) {
                    for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                        int currentTemp = Integer.parseInt(cell.getValue().toStringUtf8());
                        if (currentTemp > maxTemp) {
                            maxTemp = currentTemp;
                        }
                    }
                }
            }
        }
        return maxTemp;
    }

    // Function to delete the table
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            btAdminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Table does not exist, skipping deletion: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        Bigtable testbt = new Bigtable();
        testbt.run();
    }
}