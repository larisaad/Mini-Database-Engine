import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Database implements MyDatabase {

    private HashMap<String,Table> tables;
    private ExecutorService tpe, tpe2;



    public Database() {
        this.tables = new HashMap<>();
    }

    @Override
    public void initDb(int numWorkerThreads) {

        tpe = Executors.newFixedThreadPool(numWorkerThreads);
    }

    @Override
    public void stopDb() {
        tpe.shutdown();
    }

    @Override
    public void createTable(String tableName, String[] columnNames, String[] columnTypes) {
        tables.put(tableName, new Table(columnNames, columnTypes));

    }

    @Override
    public ArrayList<ArrayList<Object>> select(String tableName, String[] operations, String condition){
        ArrayList<ArrayList<Object>> result = null;
        try {
            result = tables.get(tableName).select(operations, condition, tpe);
        } catch (InterruptedException e) {}

        return result;
    }

    @Override
    public void update(String tableName, ArrayList<Object> values, String condition) {

        try {

            tables.get(tableName).update(values, condition, tpe);
        } catch (InterruptedException e) {}
    }

    @Override
    public void insert(String tableName, ArrayList<Object> values) {

        try {
            tables.get(tableName).insert(values);
        } catch (InterruptedException e) {}
    }

    @Override
    public void startTransaction(String tableName) {
        // cresc threadurile/taskurile
        tables.get(tableName).startTransaction();

    }

    @Override
    public void endTransaction(String tableName) {
        tables.get(tableName).endTransaction();
    }
}
