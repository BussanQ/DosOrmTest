using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Data;

namespace WinFormTest
{
    public class DBHelperOracle
    {
        /// <summary>
        /// 连接字符串
        /// </summary>
        private string connStr = string.Empty;

        /// <summary>
        /// 目标连接字符串
        /// </summary>
        //private string tarConnStr = "Data Source=10.2.111.161/orcl;User Id=atms;Password=ehl1234";

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="dbname"></param>
        /// <param name="userid"></param>
        /// <param name="pwd"></param>
        public DBHelperOracle(string dbname, string userid, string pwd)
        {
            connStr = string.Format("Data Source={0};User Id={1};Password={2};", dbname, userid, pwd);
        }

        #region 单例

        /// <summary>
        /// 单例
        /// </summary>
        private static DBHelperOracle instance = new DBHelperOracle();

        public DBHelperOracle() { }

        /// <summary>
        /// 异步控制对象
        /// </summary>
        private static object synObj = new object();

        /// <summary>
        /// 单例
        /// </summary>
        public static DBHelperOracle Instance
        {
            get
            {
                lock (synObj)
                {
                    return instance;
                }
            }
        }

        #endregion

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>DataSet</returns>
        public DataTable Query(string strSQL)
        {
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                DataTable dt = new DataTable();
                try
                {
                    DataSet ds = new DataSet();
                    connection.Open();
                    OracleDataAdapter command = new OracleDataAdapter(strSQL, connection);
                    command.Fill(ds, "ds");
                    if (ds.Tables.Count > 0)
                        dt = ds.Tables[0];
                }
                catch (OracleException ex)
                {
                    //LogHelper.WriteLog("TFM", string.Format(
                    //     "类名称：{0} 方法名称：{1} 消息：{2} sql:{3} ", "DBHelperOracle", "Query", ex.ToString(), strSQL), 0);
                }
                return dt;
            }
        }

        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="strSQL">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public object GetSingle(string strSQL)
        {
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                using (OracleCommand cmd = new OracleCommand(strSQL, connection))
                {
                    try
                    {
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (OracleException ex)
                    {
                        //LogHelper.WriteLog("TFM", string.Format(
                        // "类名称：{0} 方法名称：{1} 消息：{2} sql:{3} ", "DBHelperOracle", "GetSingle", ex.ToString(), strSQL), 0);
                        return null;
                    }
                }
            }
        }

        /// <summary>
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public int ExecuteSql(string strSQL)
        {
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                using (OracleCommand cmd = new OracleCommand(strSQL, connection))
                {
                    try
                    {
                        connection.Open();
                        int rows = cmd.ExecuteNonQuery();
                        return rows;
                    }
                    catch (OracleException ex)
                    {
                      //  LogHelper.WriteLog("TFM", string.Format(
                      //"类名称：{0} 方法名称：{1} 消息：{2} sql:{3} ", "DBHelperOracle", "ExecuteSql", ex.ToString(), strSQL), 0);
                        return -1;
                    }
                }
            }
        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="columnRowData">键-值存储的批量数据：键是列名称，值是对应的数据集合</param>
        /// <param name="columnsType">字段类型数组</param>
        /// <param name="len">数据长度</param>
        /// <returns></returns>
        public int BatchInsert(string tableName, Dictionary<string, object>
          columnRowData, string[] columnsType, int len)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException("必须指定批量插入的表名称", "tableName");
            }

            if (columnRowData == null || columnRowData.Count < 1)
            {
                throw new ArgumentException("必须指定批量插入的字段名称", "columnRowData");
            }

            int iResult = 0;
            string[] dbColumns = columnRowData.Keys.ToArray();
            StringBuilder sbCmdText = new StringBuilder();
            if (columnRowData.Count > 0)
            {
                //准备插入的SQL
                sbCmdText.AppendFormat("INSERT INTO {0}(", tableName);
                sbCmdText.Append(string.Join(",", dbColumns));
                sbCmdText.Append(") VALUES (");
                sbCmdText.Append(":" + string.Join(",:", dbColumns));
                sbCmdText.Append(")");

                using (OracleConnection conn = new OracleConnection(connStr))
                {
                    using (OracleCommand cmd = conn.CreateCommand())
                    {
                        //绑定批处理的行数
                        cmd.ArrayBindCount = len;
                        cmd.BindByName = true;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sbCmdText.ToString();
                        cmd.CommandTimeout = 600;//10分钟

                        //创建参数
                        OracleParameter oraParam;
                        List<IDbDataParameter> cacher = new List<IDbDataParameter>();
                        OracleDbType dbType = OracleDbType.Varchar2;
                        int i = 0;
                        foreach (string colName in dbColumns)
                        {
                            dbType = GetOracleDbType(columnsType[i]);
                            oraParam = new OracleParameter(colName, dbType);
                            oraParam.Direction = ParameterDirection.Input;
                            oraParam.OracleDbTypeEx = dbType;

                            oraParam.Value = columnRowData[colName];
                            cmd.Parameters.Add(oraParam);
                            i++;
                        }
                        //打开连接
                        conn.Open();

                        /*执行批处理*/
                        var trans = conn.BeginTransaction();
                        try
                        {
                            cmd.Transaction = trans;
                            iResult = cmd.ExecuteNonQuery();
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();
                            // throw ex;
                        }
                        finally
                        {
                            if (conn != null) conn.Close();
                        }

                    }
                }
            }
            return iResult;
        }

        /**
         * 根据数据类型获取OracleDbType
         */
        private static OracleDbType GetOracleDbType(string value)
        {
            OracleDbType dataType = OracleDbType.Varchar2;
            if (value == "DATE")
            {
                dataType = OracleDbType.Date;
            }
            else if (value == "VARCHAR2")
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value == "NUMBER")
            {
                dataType = OracleDbType.Decimal;
            }
            else if (value == "CHAR")
            {
                dataType = OracleDbType.Char;
            }
            else if (value == "NVARCHAR2")
            {
                dataType = OracleDbType.NVarchar2;
            }
            return dataType;
        }

        /**
          * 根据数据类型获取OracleDbType
          */
        private static OracleDbType GetOracleDbType(object value)
        {
            OracleDbType dataType = OracleDbType.Varchar2;
            if (value is string[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is DateTime[])
            {
                dataType = OracleDbType.TimeStamp;
            }
            else if (value is int[] || value is short[])
            {
                dataType = OracleDbType.Int32;
            }
            else if (value is long[])
            {
                dataType = OracleDbType.Int64;
            }
            else if (value is decimal[] || value is double[] || value is float[])
            {
                dataType = OracleDbType.Decimal;
            }
            else if (value is Guid[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is bool[] || value is Boolean[])
            {
                dataType = OracleDbType.Byte;
            }
            else if (value is byte[])
            {
                dataType = OracleDbType.Blob;
            }
            else if (value is char[])
            {
                dataType = OracleDbType.Char;
            }
            return dataType;
        }
    }
}
