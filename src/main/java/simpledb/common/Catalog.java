package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 表目录
 * 保存数据库中所有表和其关联的模式，目前由用户程序填充
 *
 * @Threadsafe
 */
public class Catalog {

    /**
     * 表格map，key=表名，value=表
     */
    private Map<String, DbFile> dbFileMap;

    /**
     * 主键map，key=表明，value=表主键名称
     */
    private Map<String, String> pkeyMap;

    /**
     * 表名map，key=表id，value=表名
     * 仅用于存储表id对应的表名，当id重复时，快速定位到对应的表
     */
    private Map<Integer, String> nameMap;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        dbFileMap = new HashMap<>();
        pkeyMap = new HashMap<>();
        nameMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * 1、name和id都不重复，直接添加即可
     * 2、name和id都重复了，由于name作为键具有唯一性，直接添加即可
     * 3、name重复，id不重复，直接添加即可
     * 4、name不重复，id重复，需要特殊处理
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     *                  存储表内容的部分
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     *                  表名，可能是空字符串，但不能为null，如果名称冲突，就覆盖已有的表
     * @param pkeyField the name of the primary key field
     *                  表主键名
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (Objects.isNull(name)) {
            // 表名不能为null
            throw new RuntimeException("table name shouldn't be null");
        }

        // 4、name不重复，id重复，需要特殊处理
        if (!dbFileMap.containsKey(name) && nameMap.containsKey(file.getId())) {
            String oldName = nameMap.get(file.getId());
            // 删除旧表和主键信息
            dbFileMap.remove(oldName);
            pkeyMap.remove(oldName);
        }
        dbFileMap.put(name, file);
        pkeyMap.put(name, pkeyField);
        nameMap.put(file.getId(), name);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 根据表名返回表id
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (dbFileMap.containsKey(name)) {
            return dbFileMap.get(name).getId();
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 根据表id，返回表的格式
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        for (Map.Entry<String, DbFile> entry : dbFileMap.entrySet()) {
            DbFile dbFile = entry.getValue();
            if (dbFile.getId() == tableid) {
                return dbFile.getTupleDesc();
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 根据表id，返回表内容
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        for (Map.Entry<String, DbFile> entry : dbFileMap.entrySet()) {
            DbFile dbFile = entry.getValue();
            if (dbFile.getId() == tableid) {
                return dbFile;
            }
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        for (Map.Entry<String, DbFile> entry : dbFileMap.entrySet()) {
            DbFile dbFile = entry.getValue();
            if (dbFile.getId() == tableid) {
                return pkeyMap.get(entry.getKey());
            }
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        List<Integer> tableIdList = new ArrayList<>();
        dbFileMap.values().forEach(dbFile -> tableIdList.add(dbFile.getId()));
        return tableIdList.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        for (Map.Entry<String, DbFile> entry : dbFileMap.entrySet()) {
            DbFile dbfile = entry.getValue();
            if (dbfile.getId() == id) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        dbFileMap.clear();
        pkeyMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

