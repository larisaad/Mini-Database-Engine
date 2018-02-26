import java.util.ArrayList;

public class Column {

    private String type;
    private ArrayList<Object> values;
    public int sum;

    public Column(String type) {
        this.type = type;
        values = new ArrayList<>();
        sum = 0;
    }

    public String getType() {
        return type;
    }

    public ArrayList<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Column{" +
                "type='" + type + '\'' +
                ", values=" + values +
                '}';
    }
}
