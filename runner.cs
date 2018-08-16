using System;
using Npgsql;
using FileHelpers;

namespace test
{
    class Program
    {
        private 

        static void Main(string[] args)
        {
            //var i = new Int32();

            //-------------------------------------------establis connections-------------------------------------------------
            //var ibmc = new System.Data.Odbc.OdbcConnection("Driver={iSeries Access ODBC Driver};System=TEST400;Uid=tstdillen;Pwd=tstdillen");
            var ibmc = new System.Data.Odbc.OdbcConnection("Driver={iSeries Access ODBC Driver};System=S7830956;Uid=PTROWBRIDG;Pwd=QQQX53@027");
            var pgc = new NpgsqlConnection("Host=ushcc10091;Port=5432;Username=ptrowbridge;Password=qqqx53!026;Database=ubm;ApplicationName=runner");
            ibmc.Open();
            pgc.Open();

            //----------------------------------------------setup commands---------------------------------------------------
            var ibmcmd = new System.Data.Odbc.OdbcCommand();
            ibmcmd.Connection = ibmc;
            //ibmcmd.CommandText = "SELECT cast(ID as int) ID, TBLN, ACTN, TS, SU, COL, REPLACE(OLDV,X'00',CHR(32)) OLDV, REPLACE(NEWV,X'00',CHR(32)) NEWV, DTYPE, LENG, SCAL FROM QGPL.TRIG_LOG_EAV WHERE ID > 236832";
            //ibmcmd.CommandText = "SELECT * FROM RLARP.OSM WHERE ITER >= '2018-08-15-11.23.42.009654'";
            //ibmcmd.CommandText = "SELECT * FROM RLARP.OSMP";
            //ibmcmd.CommandText = "SELECT * FROM LGDAT.GLDATE";
            //ibmcmd.CommandText = "SELECT * FROM LGDAT.STKMM";

            ibmcmd.CommandText = System.IO.File.ReadAllText(@"C:\Users\ptrowbridge\Documents\runner\stkmm.sql");



            var pgcmd = new NpgsqlCommand();
            pgcmd.Connection = pgc;
            //pgcmd.CommandText = "SELECT * FROM rlarp.trig_log_eav WHERE 0=1";
            //pgcmd.CommandText = "SELECT * FROM rlarp.osmi WHERE 0=1";
            //pgcmd.CommandText = "SELECT * FROM lgdat.gldate WHERE 0=1";
            pgcmd.CommandText = "SELECT * FROM lgdat.stkmm WHERE 0=1";

            //---------------------------------------------setup adapters---------------------------------------------------------
            //var ibmds = new System.Data.DataSet();
            //var ibmda = new System.Data.Odbc.OdbcDataAdapter(ibmcmd);
            //ibmda.Fill(ibmds);
            

            var pgds = new System.Data.DataSet();
            var pgda = new NpgsqlDataAdapter(pgcmd);
            pgda.Fill(pgds);
            //pgda.UpdateBatchSize = 100;

            Console.Write("etl start:" + DateTime.Now.ToString());
            Console.Write(Environment.NewLine);

            //--------------------------------------------move to target--------------------------------------------------------
            var ibmdr = ibmcmd.ExecuteReader();
            var getv = new object[ibmdr.FieldCount];
            int r = 0;
            string sql = "";
            string nr = "";
            string nc = "";
            var pgcom = pgc.CreateCommand();
            while (ibmdr.Read()) { 
                r = r + 1;
                nr = "";  
                /*  
                for ( int i = 0 ; i < ibmdr.GetValues(getv);i++) {
                    if (getv[i] != null) {
                        switch (ibmdr.GetDataTypeName(i)){
                            case "VARCHAR":
                                nc = "'" + getv[i].ToString().Replace("'","''") + "'"; 
                                break;
                            case "CHAR":
                                nc = "'" + getv[i].ToString().Replace("'","''") + "'";
                                break;
                            case "DATE":
                                if (getv[i].ToString() != "1/1/0001 12:00:00 AM") {
                                    nc = "'" + getv[i].ToString() + "'";
                                }
                                else {
                                    nc = "NULL";
                                }
                                break;
                            case "TIME":
                                nc = "'" + getv[i].ToString() + "'";
                                break;
                            case "TIMESTAMP":
                                nc = "'" + getv[i].ToString() + "'";
                                break;
                            default:
                                nc = getv[i].ToString();
                                break;
                        }
                    }
                    else { 
                        nc = "NULL";
                    }
                    if (i!=0) {
                        nr = nr + ",";
                    }
                    nr = nr + nc;
                }
                if (sql!="") {
                    sql = sql + ",";
                }
                sql = sql + "(" + nr + ")";
                if (r == 500) {
                    r = 0;
                    pgcom.CommandText = "INSERT INTO lgdat.stkmm VALUES " + sql;
                    //pgcom.ExecuteNonQuery();
                    sql = "";
                }
                */
            }
            if (r != 0) {
                pgcom.CommandText = "INSERT INTO lgdat.stkmm VALUES " + sql;
                //pgcom.ExecuteNonQuery();
                sql = "";          
            }

            ibmc.Close();
            pgc.Close();

            Console.Write("etl end:" + DateTime.Now.ToString());
            
        }
    }
}
