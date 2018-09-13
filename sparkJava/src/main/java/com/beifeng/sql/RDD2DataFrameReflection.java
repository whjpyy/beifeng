package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 使用反射的方式将RDD转换为DataFrame
 * time: 2018-09-09 21:21
 */
public class RDD2DataFrameReflection {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("D:\\sparkTestFile\\students.txt");

        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] lineSplited = s.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(lineSplited[0]));
                stu.setName(lineSplited[1]);
                stu.setAge(Integer.valueOf(lineSplited[2]));
                return stu;
            }
        });

        // 使用反射方式，将RDD转换为DataFrame
        // 将Student.class传入进去，其实就是用反射的方式创建DataFrame
        // 因为Student.class本身就是反射的一个应用
        // 然后底层还得通过对Student Class 进行反射，来获取其中的field
        // 这里要求，JavaBean必须实现Serialable接口，是可序列化的
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);

        // 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行sql语句
        studentDF.registerTempTable("students");

        // 查询年龄小于18岁的学生
        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        // 将RDD中的数据，进行映射为Student
        JavaRDD<Student> teenagerStudent = teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                stu.setAge(row.getInt(0));
                return stu;
            }
        });

        // 将数据collecthuil, 打印出来
        List<Student> studentList = teenagerStudent.collect();
        for(Student stu: studentList){
            System.out.println(stu);
        }

    }
}