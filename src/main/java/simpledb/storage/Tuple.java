package simpledb.storage;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 * 关系型数据库中，关系是一张表，表中每行数据就是元组
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 元组描述
     */
    private TupleDesc td;

    /**
     * 元组位置
     */
    private RecordId rid;

    /**
     * 元组中字段值列表
     */
    private List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        if (Objects.isNull(td) || td.numFields() < 1) {
            throw new RuntimeException("初始化失败，请检查参数");
        }
        this.td = td;
        this.fields = new ArrayList<>(td.numFields());
        // 赋默认值为null
        for (int i = 0; i < td.numFields(); i++) {
            this.fields.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= fields.size()) {
            throw new RuntimeException("入参i非法异常");
        }

        if (!Objects.equals(f.getType(), td.getFieldType(i))) {
            throw new RuntimeException("入参f类型异常");
        }

        fields.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.size()) {
            throw new RuntimeException("入参i非法异常");
        }
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i));
            if (i < fields.size() - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        Iterator<Field> iterator = null;
        if (Objects.nonNull(fields)) {
            iterator = fields.iterator();
        }
        return iterator;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.td = td;
    }
}
