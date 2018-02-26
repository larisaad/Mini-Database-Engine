import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

public class UpdateRunnable  implements Runnable{

    ExecutorService tpe;
    int step;
    int stopStep;
    MyCondition condition;
    ArrayList<Object> updateValues;
    HashMap<String, Column> columns;
    String[] columnNames;

    public UpdateRunnable(ExecutorService tpe, int step, int stopStep, MyCondition condition, ArrayList<Object> updateValues,
                          HashMap<String, Column> columns, String[] columnNames) {
        this.tpe = tpe;
        this.step = step;
        this.stopStep = stopStep;
        this.condition = condition;
        this.updateValues = updateValues;
        this.columnNames = columnNames;
        this.columns = columns;

    }

    @Override
    public void run() {

                Column matchColumn = condition.getColumn();
                ArrayList<Object> matchValues = matchColumn.getValues();
                for (int i = step; i < stopStep; i++) {
                    if (condition.isMatching(matchValues.get(i))) {
                        for (int j = 0; j < columnNames.length; j++) {
                            ArrayList<Object> values = columns.get(columnNames[j]).getValues();
                            values.set(i, updateValues.get(j));
                    }
                }

        }

    }

}
