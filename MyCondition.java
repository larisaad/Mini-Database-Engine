public class MyCondition {

    private Column column;
    private String comparator;
    private String value;

    public MyCondition(Column column, String comparator, String value) {
        this.column = column;
        this.comparator = comparator;
        this.value = value;
    }

    public boolean isMatching(Object o) {
        if (column.getType() == "int") return isIntMatching((Integer)o);
        else if (column.getType() == "bool") return isBoolMatching((Boolean)o);
        else return isStringMatching((String)o);
    }

    public boolean isIntMatching(Integer integer) {
        switch (comparator) {
            case ">": return integer > Integer.parseInt(value);
            case "<": return integer < Integer.parseInt(value);
            case "==": return integer == Integer.parseInt(value);
            default: return true;
        }
    }
    public boolean isBoolMatching(Boolean bool) {
        switch (comparator) {
            case "==": return bool == Boolean.parseBoolean(value);
            default: return true;
        }
    }
    public boolean isStringMatching(String string) {
        switch (comparator) {
            case "==": return string.equals(value);
            default: return true;
        }
    }
    public Column getColumn() {
        return column;
    }
}
