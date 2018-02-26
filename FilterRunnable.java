
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class FilterRunnable implements Runnable {

    ExecutorService tpe;
    int step;
    int stopStep;
    MyCondition condition;
    Column column;
    String operation;

    public FilterRunnable(ExecutorService tpe, int step, int stopStep, Column column, MyCondition condition, String operation) {
        this.tpe = tpe;
        this.step = step;
        this.stopStep = stopStep;
        this.condition = condition;
        this.column = column;
        this.operation = operation;
    }

    @Override
    public void run() {
        switch (operation) {
            case "sum":
                ArrayList<Object> values = column.getValues();
                Column matchColumn = condition.getColumn();
                ArrayList<Object> matchValues = matchColumn.getValues();
                for (int i = step; i < stopStep; i++) {
                    if (condition.isMatching(matchValues.get(i))) {
                        column.sum += (Integer) values.get(i);

                    }
                }
                break;


        }

    }

}
