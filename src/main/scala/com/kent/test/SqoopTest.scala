package com.kent.test

/*import org.apache.sqoop.util.OptionsFileUtil
import org.apache.sqoop.tool.SqoopTool
import org.apache.hadoop.conf.Configuration
import org.apache.sqoop.Sqoop
import org.apache.sqoop.tool.ImportTool
import org.apache.sqoop.SqoopOptions
import org.apache.hadoop.fs.Path*/

object SqoopTest extends App{
/*  
  def f1() = {
    val ags = Array(
                "--connect","jdbc:mysql://192.168.31.245:3306/wf?useSSL=false",
                "--driver","com.mysql.jdbc.Driver",
                "-username","root",
                "-password","root",
                "--table","test_kpi_day_data",
                "-m","1",
                "--target-dir","/user/ogn/workflow/data2"           
        )
    val expandArguments = OptionsFileUtil.expandArguments(ags);
    val tool = SqoopTool.getTool("import");

    val conf = new Configuration();
    conf.set("fs.default.name", "hdfs://192.168.31.223:8020");//设置HDFS服务地址
    val loadPlugins = SqoopTool.loadPlugins(conf);
    
    val sqoop = new Sqoop(tool.asInstanceOf[com.cloudera.sqoop.tool.SqoopTool], loadPlugins);
    println(Sqoop.runSqoop(sqoop, expandArguments));
  }
  
  def f2() = {
    val ags = Array(
                "--connect","jdbc:mysql://192.168.31.245:3306/wf?useSSL=false",
                "--driver","com.mysql.jdbc.Driver",
                "-username","root",
                "-password","root",
                "--table","test_kpi_day_data",
                "-m","1",
                "--target-dir","/user/ogn/workflow/data3"           
        )
    val expandArguments = OptionsFileUtil.expandArguments(ags)
    val importer = new ImportTool()
    val sqoop = new Sqoop(importer)
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs://192.168.31.223:8020")
    conf.set("yarn.resourcemanager.address", "quickstart.cloudera:8032")
    sqoop.setConf(conf)
    Sqoop.runSqoop(sqoop, ags)
  }
  def f4(){
    val config = new Configuration(); 
    //config.addResource(new Path("/conf/core-site.xml"))
    //config.addResource(new Path("/conf/hdfs-site.xml"))
    config.set("fs.default.name", "hdfs://192.168.31.223:8020")
    
    val ret = Sqoop.runTool(Array(
               "import",
                "--connect","jdbc:mysql://192.168.31.245:3306/wf?useSSL=false",
                "--driver","com.mysql.jdbc.Driver",
                "-username","root",
                "-password","root",
                "--table","test_kpi_day_data",
                "-m","1",
                "--target-dir","/user/ogn/workflow/data2"));
    if (ret != 0) {
      throw new RuntimeException("Sqoop failed - return code " + Integer.toString(ret));
    }
  }
  
  f2()*/
}