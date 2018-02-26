import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table {

    private String[] columnNames;
    private HashMap<String, Column> columns;
    private int noInserts;

    private final ReadWriteLock rwLock;
    private final Lock readLock, writeLock;


    public Table(String[] columnNames, String[] columnTypes) {

        this.columnNames = columnNames;
        columns = new HashMap<>();

        for (int i = 0; i < columnNames.length; i++) {
            columns.put(columnNames[i], new Column(columnTypes[i]));
        }
        rwLock = new ReentrantReadWriteLock(true);
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();

    }

    public void submitFilterJobs(ExecutorService tpe, Column column, MyCondition condition, String operation) {

        List<Future<?>> futureList = new ArrayList<>();
        int dimTask = noInserts / 20;
        int start = 0;
        if (noInserts < 20) { dimTask = 1;}
        int stop = start + dimTask;

        int end;
        if (noInserts < 20) { end = noInserts + 1;}
        else {end = 20;}

        for (int i = 0; i < end; i++) {
            Future<?> future = tpe.submit(new FilterRunnable(tpe, start, stop, column, condition, operation));
            futureList.add(future);
            start = stop;
            if (i == 18) { stop = noInserts; }
            else { stop += dimTask; }
        }
        boolean allDone;
        do {
            for (Future<?> future : futureList) {
                try {
                    future.get();
                } catch (InterruptedException ie) {
                } catch (ExecutionException e) {
                }
            }

            allDone = true;
            for (Future<?> future : futureList) {
                allDone &= future.isDone();
            }
        }
        while(!allDone);


    }

    public void submitUpdateJobs(ExecutorService tpe, MyCondition condition, ArrayList<Object> updateValues) {

        List<Future<?>> futureList = new ArrayList<>();
        int dimTask = noInserts / 20;
        int start = 0;
        if (noInserts < 20) { dimTask = 1;}
        int stop = start + dimTask;

        int end;
        if (noInserts < 20) { end = noInserts + 1;}
        else {end = 20;}

        for (int i = 0; i < end; i++) {
            Future<?> future = tpe.submit(new UpdateRunnable(tpe, start, stop, condition, updateValues, columns, columnNames ));
            futureList.add(future);
            start = stop;
            if (i == 18) { stop = noInserts; }
            else { stop += dimTask; }
        }
        boolean allDone;
        do {
            for (Future<?> future : futureList) {
                try {
                    future.get();
                } catch (InterruptedException ie) {
                } catch (ExecutionException e) {
                }
            }

            allDone = true;
            for (Future<?> future : futureList) {
                allDone &= future.isDone();
            }
        }
        while(!allDone);


    }
    /* reading operation*/

    public ArrayList<ArrayList<Object>> select(String[] operations, String condition, ExecutorService tpe) throws InterruptedException {

        readLock.lock();
        ArrayList<ArrayList<Object>> result = new ArrayList<>();

        try {

            // solve condition
            String[] atoms = condition.split(" ");
            MyCondition cond;
            if (atoms.length > 1) {
                cond = new MyCondition(columns.get(atoms[0]), atoms[1], atoms[2]);
            } else { //empty string, no restriction
                cond = new MyCondition(null, "", "");
            }

            // having the condition, do operations
            for (int i = 0; i < operations.length; i++) {
                ArrayList<Object> operationResult = new ArrayList<>();
                String[] tokens = operations[i].split("\\(|\\)");
                if (tokens.length == 1) // only name column
                {
                    //select name column
                    operationResult = allColumn(tokens[0], cond);

                } else {
                    switch (tokens[0]) {
                        case "min":
                            operationResult.add(minColumn(tokens[1], cond));
                            break;
                        case "max":
                            operationResult.add(maxColumn(tokens[1], cond));
                            break;
                        case "count":
                            operationResult.add(countColumn(tokens[1], cond));
                            break;
                        case "avg":
                            operationResult.add(sumColumn(tokens[1], cond) / countColumn(tokens[1], cond));
                            break;
                        case "sum":
                            //columns.get(tokens[1]).sum = 0;
                           // submitFilterJobs(tpe, columns.get(tokens[1]), cond, "sum");
                            //operationResult.add(columns.get(tokens[1]).sum);
                           operationResult.add(sumColumn(tokens[1], cond));
                            break;

                    }
                }
                result.add(operationResult);

            }
        } finally {
            readLock.unlock();
        }

        return result;
    }

    public void insert(ArrayList<Object> values) throws InterruptedException {
        writeLock.lock();
        try {
            noInserts++;
            for (int i = 0; i < columnNames.length; i++) {
                columns.get(columnNames[i]).getValues().add(values.get(i));
            }
        } finally {
            writeLock.unlock();
        }

    }

    public void update(ArrayList<Object> values, String condition, ExecutorService tpe) throws InterruptedException {

        writeLock.lock();
        try {
            // solve condition
            String[] atoms = condition.split(" ");
            MyCondition cond;
            if (atoms.length > 1) {
                cond = new MyCondition(columns.get(atoms[0]), atoms[1], atoms[2]);
            } else { //empty string, no restriction
                cond = new MyCondition(null, "", "");
            }

            submitUpdateJobs(tpe, cond, values);
        } finally {
            writeLock.unlock();
        }
    }
    public ArrayList<Object> allColumn (String nameColumn, MyCondition cond) {

        ArrayList<Object> result = new ArrayList<>();

        ArrayList<Object> values = columns.get(nameColumn).getValues();
        Column matchColumn = cond.getColumn();
        ArrayList<Object> matchValues = matchColumn.getValues();
        for (int i = 0; i < values.size(); i++) {
            if (cond.isMatching(matchValues.get(i))) {
                result.add(values.get(i));
            }
        }
        return result;
    }

    public Integer countColumn (String nameColumn, MyCondition cond) {

        ArrayList<Object> result = new ArrayList<>();
        int count = 0;
        ArrayList<Object> values = columns.get(nameColumn).getValues();
        Column matchColumn = cond.getColumn();
        ArrayList<Object> matchValues = matchColumn.getValues();
        for (int i = 0; i < values.size(); i++) {
            if (cond.isMatching(matchValues.get(i))) {
                count++;
            }
        }
        return count;
    }


    public Integer minColumn(String nameColumn, MyCondition cond) {

        Column column = columns.get(nameColumn);
        if (column.getType() != "int") return null;

        Integer min = Integer.MAX_VALUE;
        ArrayList<Object> values = column.getValues();
        Column matchColumn = cond.getColumn();
        ArrayList<Object> matchValues = matchColumn.getValues();

        for (int i = 0; i < values.size(); i++) {
            if (cond.isMatching(matchValues.get(i))) {
                if ((Integer) values.get(i) < min) {
                    min = (Integer) values.get(i);
                }
            }
        }
        return min;
    }

    public Integer maxColumn(String nameColumn, MyCondition cond) {

        Column column = columns.get(nameColumn);
        if (column.getType() != "int") return null;

        Integer max = Integer.MIN_VALUE;
        ArrayList<Object> values = column.getValues();

        Column matchColumn = cond.getColumn();
        ArrayList<Object> matchValues = matchColumn.getValues();

        for (int i = 0; i < values.size(); i++) {
            if (cond.isMatching(matchValues.get(i))) {
                if ((Integer) values.get(i) > max) {
                    max = (Integer) values.get(i);
                }
            }
        }
        return max;
    }

    public Integer sumColumn(String nameColumn, MyCondition cond) {

        Column column = columns.get(nameColumn);
        if (column.getType() != "int") return null;
        Integer sum = 0;

        ArrayList<Object> values = column.getValues();
        Column matchColumn = cond.getColumn();
        ArrayList<Object> matchValues = matchColumn.getValues();

        for (int i = 0; i < values.size(); i++) {
            if (cond.isMatching(matchValues.get(i))) {
                sum += (Integer) values.get(i);
            }
        }
        return sum;
    }
    public void startTransaction() {

       writeLock.lock();
    }

    public void endTransaction() {
        writeLock.unlock();
    }

    @Override
    public String toString() {
        return "Table{" +
                "columns=" + columns +
                '}';
    }
}