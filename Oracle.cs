using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WinFormTest
{
    public class Oracle
    {
        
        private Oracle() { }
        public Oracle(string Host, string Port, string Service_Name,string UserID,string PassWord)
        {
            connStr= string.Format(@"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)
                    (HOST={0})(PORT={1})))(CONNECT_DATA=(SERVICE_NAME={2})));Persist Security Info=True;User ID={3};Password={4}",
                   Host,
                   Port,
                   Service_Name,
                   UserID,
                   PassWord);
        }

        public static string connStr = string.Empty;
        private static Oracle instance = new Oracle();
        private static object synObj = new object();

        /// <summary>
        /// 单例
        /// </summary>
        public static Oracle Instance
        {
            get
            {
                lock (synObj)
                {
                    if (instance == null)
                        instance = new Oracle();
                }
                return instance;
            }
        }

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>DataSet</returns>
        public DataSet Query(string strSQL)
        {
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    OracleDataAdapter command = new OracleDataAdapter(strSQL, connection);
                    command.Fill(ds, "ds");
                    connection.Close();
                }
                catch (Exception ex)
                {
                    connection.Close();
                   return null;
                }
                return ds;
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
                connection.Open();
                try
                {
                    using (OracleCommand cmd = new OracleCommand(strSQL, connection))
                    {
                        int rows = cmd.ExecuteNonQuery();
                        connection.Close();
                        return rows;
                    }
                }
                catch (Exception ex)
                {
                    connection.Close();
                    return -1;
                }
            }
        }


        /// <summary>
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public int ExecuteSql(IList<string> sqlList)
        {
            int result = 0;
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                try
                {
                    connection.Open();
                    OracleTransaction oraTran = connection.BeginTransaction();
                    foreach (string strSQL in sqlList)
                    {

                        //using (OracleCommand cmd = new OracleCommand(strSQL, connection, oraTran))
                        //{
                        //    try
                        //    {
                        //        result += cmd.ExecuteNonQuery();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        oraTran.Rollback();
                        //        connection.Close();
                        //        return -1;
                        //    }
                        //}
                        using (OracleCommand cmd = new OracleCommand(strSQL, connection))
                        {
                            try
                            {
                                cmd.Transaction = oraTran;
                                result += cmd.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                oraTran.Rollback();
                                connection.Close();
                                return -1;
                            }
                        }
                    }
                    oraTran.Commit();
                    connection.Close();
                }
                catch (Exception ex)
                {
                    connection.Close();
                   return -1;
                }
            }
            return result;
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
                        connection.Close();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (Exception ex)
                    {
                        connection.Close();
                        return null;
                    }
                }
            }
        }
    }
}
