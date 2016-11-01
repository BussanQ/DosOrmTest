using Dos.ORM;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WinFormTest
{
    public class DB
    {
        //static string conn = ConfigurationManager.AppSettings["MySqlConn"];
        public static readonly DbSession sourceContext = new DbSession(DatabaseType.Oracle,
            "Data Source=10.2.111.161/orcl;User Id=ehl_tfm;Password=ehl1234;");
        public static readonly DbSession targetContext = new DbSession(DatabaseType.Oracle,
          "Data Source=10.2.111.161/orcl;User Id=atms;Password=ehl1234;");
        public static readonly DbSession mysqlContext = new DbSession("MySqlConn");

        private DBHelperOracle sourceOracleHelper = 
            new DBHelperOracle("10.2.111.161/orcl", "ehl_tfm", "ehl1234");
        private DBHelperOracle targetOracleHelper =
           new DBHelperOracle("10.2.111.161/orcl", "atms", "ehl1234");

        private Oracle sourceOracle = new Oracle("10.2.111.161","1521", "orcl","ehl_tfm", "ehl1234");
        private Oracle targetOracle = new Oracle("10.2.111.161", "1521", "orcl", "atms", "ehl1234");

        public string start()
        {
            // var mysqllist = DB.mysqlContext.FromSql("SELECT * FROM t_tfm_link_dir").ToList<Model>();
            Stopwatch sw = new Stopwatch();
            
            var list = DB.sourceContext.FromSql(@"SELECT id,datetime,volume,speed,timeopy,headtime,headdistance
                from t_tfm_crossdir_5m_flow   where rownum < 100000 ").
                ToList<CROSSDIR5MFLOW>();
            List<string> sqlList = new List<string>();
           
            int cou = 0;
            foreach (var flow in list)
            {
                string sql = string.Format(@"INSERT INTO t_tfm_crossdir_5m_flow (id,datetime,volume,speed,timeopy,
                headtime,headdistance) values ('{0}', to_date('{1}', 'yyyy-mm-dd hh24:mi:ss'),{2},{3},{4},{5},
                    {6})", flow.id, flow.datetime, flow.volume, flow.speed, flow.timeopy, flow.headtime,
                    flow.headdistance);
                sqlList.Add(sql);
                //cou += DB.targetContext.FromSql(sql).ExecuteNonQuery();
            }
            sw.Start();
            int count= targetOracle.ExecuteSql(sqlList);

           // var count = DB.targetContext.Insert(list);
            sw.Stop();
            return "Dos.ORM插入" + count + "条执行时间：" + sw.ElapsedMilliseconds;
        }

        public string startOracle()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string[] columnsName = new string[] { "ID","DATETIME","VOLUME","SPEED","TIMEOPY","HEADTIME",
            "HEADDISTANCE"};
            string[] columnsType = new string[] { "VARCHAR2","DATE","NUMBER","NUMBER","NUMBER","NUMBER",
            "NUMBER"};
            int count= UpdateTargetData("T_TFM_CROSSDIR_5M_FLOW", columnsName, columnsType, 1, 100000);
            sw.Stop();
            return "oracle插入" + count + "条执行时间：" + sw.ElapsedMilliseconds;
        }


        private int UpdateTargetData(string tableName, string[] columnsName, string[] columnsType, int startIndex, int count)
        {
            try
            {
                string columns = string.Join(",", columnsName);
                string sql = string.Format(@" select {0} from (select {0}, rownum rn from {1}  
                where rownum < {2}+{3}  )  where rn >={2}", columns, tableName, startIndex, count);
                DataTable dt = sourceOracleHelper.Query(sql);
                Dictionary<string, object> columnRowData = new Dictionary<string, object>();
                foreach (string column in columnsName)
                {
                    object[] nums = dt.AsEnumerable().Select(d => d.Field<object>(column)).ToArray();
                    columnRowData.Add(column, nums);
                }
                return targetOracleHelper.BatchInsert(tableName, columnRowData, columnsType, dt.Rows.Count);
            }
            catch (Exception ex)
            {               
                return 0;
            }
        }
    }

}
