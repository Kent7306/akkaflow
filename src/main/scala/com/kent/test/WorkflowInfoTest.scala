package com.kent.test

import com.kent.workflow.WorkflowInfo

object WorkflowInfoTest extends App{
  val str = """
    <work-flow name="DW_3C_DIM_SALE_ALL_SKU_MARK"  dir="/3C/DW/SALE/" 
    mail-receivers="gzouguangneng@corp.netease.com" creator="区广能"
    desc="商品统一标识码与各个渠道的商品映射关系">

    <coordinator>    
        <depend-list cron="*/30 * * * *">
            <workflow name="wf_import_item" />
            <workflow name="wf_import_order" />
        </depend-list>
        <param-list>
            <param name="stadate" value="${time.today|yyyy-MM-dd hh}"/>
        </param-list>
    </coordinator>
    
    <start name="start" to="script" />

    <action name="script" desc = "脚本执行">
        <script>
          <code><![CDATA[
#商品统一标识码与各个渠道的商品映射关系 DW_3C_DIM_SALE_ALL_SKU_MARK
ORACLE -e "
drop table DW_3C_DIM_SALE_ALL_SKU_MARK;
create table DW_3C_DIM_SALE_ALL_SKU_MARK as
select origin,mark,skuid,skuname,price,up_date from DW_3C_DIM_SALE_YX_ALL_SKU_MARK where origin in ('yx','yqp','yx_others')
union all
select 'kl' origin,mark,skuid,skuname,price,up_date from DW_3C_DIM_SALE_KL_SKU_MARK
union all
select 'jd-pf' origin,mark,skuid,skuname,price,up_date from DW_3C_DIM_SALE_JD_SKU_MARK
union all
select origin,mark,skuid,skuname,price,up_date from DW_3C_DIM_SALE_INNER_SKU_MARK
union all
select 'tmall' origin,mark,skuid,skuname,price,up_date from DW_3C_DIM_SALE_TMALL_SKU_MARK
"
            ]]></code>
        </script>
        <ok to="end"/>
    </action>

    <end name="end"/>
</work-flow>
    """
  
  val wf = WorkflowInfo(str)
  println(wf.coorOpt)
}