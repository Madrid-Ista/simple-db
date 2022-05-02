package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 * 描述一个元组的格式
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * 字段类型
         */
        public final Type fieldType;

        /**
         * The name of the field
         * 字段名
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> iterator = null;
        if (Objects.nonNull(tdItems)) {
            iterator = tdItems.iterator();
        }
        return iterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * 字段列表
     */
    private List<TDItem> tdItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (Objects.nonNull(typeAr) && Objects.nonNull(fieldAr) && typeAr.length == fieldAr.length
                && typeAr.length > 0) {
            // 元组长度，即每行数据的字段数量
            int tupleLength = typeAr.length;
            tdItems = new ArrayList<>(tupleLength);
            for (int i = 0; i < typeAr.length; i++) {
                TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
                tdItems.add(tdItem);
            }
        } else {
            throw new RuntimeException("初始化元组描述类异常");
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (Objects.nonNull(typeAr) && typeAr.length > 0) {
            // 元组长度，即每行数据的字段数量
            int tupleLength = typeAr.length;
            tdItems = new ArrayList<>(tupleLength);
            for (Type type : typeAr) {
                TDItem tdItem = new TDItem(type, null);
                tdItems.add(tdItem);
            }
        } else {
            throw new RuntimeException("初始化元组描述类异常");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0) {
            throw new NoSuchElementException();
        }
        String fieldName = null;
        if (Objects.nonNull(tdItems) && i < tdItems.size()) {
            fieldName = tdItems.get(i).fieldName;
        } else {
            throw new NoSuchElementException();
        }
        return fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0) {
            throw new NoSuchElementException();
        }
        Type fieldType = null;
        if (Objects.nonNull(tdItems) && i < tdItems.size()) {
            fieldType = tdItems.get(i).fieldType;
        } else {
            throw new NoSuchElementException();
        }
        return fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (Objects.isNull(name)) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < tdItems.size(); i++) {
            if (Objects.equals(name, tdItems.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int byteSize = 0;
        if (Objects.nonNull(tdItems)) {
            for (TDItem tdItem : tdItems) {
                byteSize = tdItem.fieldType.getLen() + byteSize;
            }
        }
        return byteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<Type> typeList = new ArrayList<>();
        List<String> fieldList = new ArrayList<>();
        if (Objects.nonNull(td1)) {
            Iterator<TDItem> iterator = td1.iterator();
            while (iterator.hasNext()) {
                TDItem tdItem = iterator.next();
                typeList.add(tdItem.fieldType);
                fieldList.add(tdItem.fieldName);
            }
        }
        if (Objects.nonNull(td2)) {
            Iterator<TDItem> iterator = td2.iterator();
            while (iterator.hasNext()) {
                TDItem tdItem = iterator.next();
                typeList.add(tdItem.fieldType);
                fieldList.add(tdItem.fieldName);
            }
        }
        Type[] types = typeList.toArray(new Type[0]);
        String[] fields = fieldList.toArray(new String[0]);
        return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (Objects.isNull(o)) {
            return false;
        }

        boolean result = true;
        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if (Objects.isNull(this.tdItems) && Objects.nonNull(other.tdItems)) {
                result = false;
            } else if (Objects.nonNull(this.tdItems) && Objects.isNull(other.tdItems)) {
                result = false;
            } else if (Objects.nonNull(this.tdItems) && Objects.nonNull(other.tdItems)) {
                if (this.tdItems.size() != other.tdItems.size()) {
                    result = false;
                } else {
                    List<TDItem> tdItemsA = this.tdItems;
                    List<TDItem> tdItemsB = other.tdItems;
                    for (int i = 0; i < tdItemsA.size(); i++) {
                        if (!tdItemsA.get(i).fieldType.equals(tdItemsB.get(i).fieldType)) {
                            result = false;
                            break;
                        }
                    }
                }
            }
        } else {
            result = false;
        }
        return result;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        tdItems.forEach(tdItem -> {
            sb.append(tdItem.fieldType).append("(").append(tdItem.fieldName).append(")");
        });
        return sb.toString();
    }
}
