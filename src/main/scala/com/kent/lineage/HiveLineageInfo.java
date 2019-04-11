package com.kent.lineage;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * This class prints out the lineage info. It takes sql as input and prints
 * lineage info. Currently this prints only input and output tables for a given
 * sql. Later we can expand to add join tables etc.
 *
 */
public class HiveLineageInfo implements NodeProcessor {

  /**
   * Stores input tables in sql.
   */
  TreeSet<String> inputTableList = new TreeSet<String>();
  /**
   * Stores output tables in sql.
   */
  TreeSet<String> OutputTableList = new TreeSet<String>();

  TreeSet<String> withTableList = new TreeSet<String>();
  
  String dbName = null;
  /**
   *
   * @return java.util.TreeSet
   */
  public TreeSet<String> getInputTableList() {
    return inputTableList;
  }

  /**
   * @return java.util.TreeSet
   */
  public TreeSet<String> getOutputTableList() {
    return OutputTableList;
  }
  
  public TreeSet<String> getWithTableList() {
	    return withTableList;
  }
  public String getSwitchDbName() {
	  return dbName;  
  }

  /**
   * Implements the process method for the NodeProcessor interface.
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    ASTNode pt = (ASTNode) nd;

    switch (pt.getToken().getType()) {
  //create语句
    case HiveParser.TOK_CREATETABLE: {
        String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
        OutputTableList.add(createName);
        break;
    }
    //insert语句
    case HiveParser.TOK_TAB:
      OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode)pt.getChild(0)));
      break;
    //from语句
    case HiveParser.TOK_TABREF:
      ASTNode tabTree = (ASTNode) pt.getChild(0);
      String table_name = (tabTree.getChildCount() == 1) ?
          BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) :
            BaseSemanticAnalyzer.getUnescapedName((ASTNode)tabTree.getChild(0)) + "." + tabTree.getChild(1);
      inputTableList.add(table_name);
      break;
   // with.....语句
    case HiveParser.TOK_CTE: {
        for (int i = 0; i < pt.getChildCount(); i++) {
            ASTNode temp = (ASTNode) pt.getChild(i);
            String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
            withTableList.add(cteName);
        }
        break;
    }
    case HiveParser.TOK_SWITCHDATABASE: {
    	    String useDBName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
        //OutputTableList.add(useDBName);
    	    dbName = useDBName;
    	    break;
    }
    }
    return null;
  }

  /**
   * parses given query and gets the lineage info.
   *
   * @param query
   * @throws ParseException
   */
  public void getLineageInfo(String query) throws ParseException,
      SemanticException {

    /*
     * Get the AST tree
     */
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(query);

    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (ASTNode) tree.getChild(0);
    }

    /*
     * initialize Event Processor and dispatcher.
     */
    inputTableList.clear();
    OutputTableList.clear();
    withTableList.clear();
    dbName = null;
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(tree);
    ogw.startWalking(topNodes, null);
  }

  public static void main(String[] args) throws IOException, ParseException,
      SemanticException {

    //String query = "create table aaa as with tmp as (select * from admin.aaa)select userid,level from t_user a join tmp b on a.userid = b.id";
    //String query = "select * from tag.loger";
    String query = "insert overwrite table aa select * from bb";
	//String query = "with tmp as (select * from admin.aaa join aaa dd)"
    	//	+ "select userid,level from t_user a join log_record b on a.userid = b.id";
    //String query = "create table admin.aaa as select * from bbb a join ccc cc on a.dfd = cc.dfd";
    //	String query = "with q1 as ( select key from src where key = '5'), q2 as ( select key from with1 a inner join with2 b on a.id = b.id) insert overwrite table temp.dt_mobile_play_d_tmp2 partition(dt='2018-07-17') select * from q1 cross join q2";
    
    HiveLineageInfo lep = new HiveLineageInfo();

    lep.getLineageInfo(query);

    for (String tab : lep.getInputTableList()) {
      System.out.println("InputTable=" + tab);
    }

    for (String tab : lep.getOutputTableList()) {  
      System.out.println("OutputTable=" + tab);
    }
    for (String tab : lep.getWithTableList()) {
        System.out.println("WithTable=" + tab);
    }
    System.out.println("switchDB=" + lep.getSwitchDbName());
    
  }
}
