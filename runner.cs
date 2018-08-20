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
            
            string scs = ""; //= "Driver={iSeries Access ODBC Driver};System=S7830956;Uid=PTROWBRIDG;Pwd=QQQX53@027";
            //string scs = ""; //= "Driver={iSeries Access ODBC Driver};System=TEST400;Uid=tstdillen;Pwd=tstdillen"
            string dcs = ""; //= "Host=ushcc10091;Port=5432;Username=ptrowbridge;Password=qqqx53!026;Database=ubm;ApplicationName=runner";
            string sq = ""; //= System.IO.File.ReadAllText(@"C:\Users\ptrowbridge\Documents\runner\osm.sql");
            string dt = ""; //= "rlarp.omsi";
            int r = 0;
            int t = 0;
            string sql = "";
            string nr = "";
            string nc = "";

            string msg = "Help:";
            msg = msg + Environment.NewLine;
            msg = msg + "version 0.13";
            msg = msg + Environment.NewLine;
            msg = msg + "-scs       source connection string";
            msg = msg + Environment.NewLine;
            msg = msg + "-dcs       destination connection string";
            msg = msg + Environment.NewLine;
            msg = msg + "-sq        path to source query";
            msg = msg + Environment.NewLine;
            msg = msg + "-dt        fully qualified name of destination table";
            msg = msg + Environment.NewLine;
            msg = msg + "--help     info";

            //---------------------------------------parse args into variables-------------------------------------------------

            for (int i = 0; i < args.Length; i = i +1 ){
                switch (args[i]) {
                    //source connection string
                    case "-scs":
                        scs = args[i+1];
                        break;
                    //destination connection string
                    case "-dcs":
                        dcs = args[i+1];
                        break;
                    //source query path
                    case "-sq":
                        sq = System.IO.File.ReadAllText(args[i+1]);
                        break;
                    //destination table name
                    case "-dt":
                        dt = "INSERT INTO " + args[i+1] + " VALUES ";
                        break;
                    case "--help":
                        Console.Write(Environment.NewLine);
                        Console.Write(msg);
                        return;
                    case "-help":
                        Console.Write(Environment.NewLine);
                        Console.Write(msg);
                        return;
                    case "-h":
                        Console.Write(Environment.NewLine);
                        Console.Write(msg);
                        return;
                    case @"\?":
                        Console.Write(Environment.NewLine);
                        Console.Write(msg);
                        return;
                    default:
                        break;
                }
            }

            Console.Write(Environment.NewLine);
            Console.Write(scs);
            Console.Write(Environment.NewLine);
            Console.Write(dcs);
            Console.Write(Environment.NewLine);
            Console.Write(sq);
            Console.Write(Environment.NewLine);
            Console.Write(dt);

            //return;

            //-------------------------------------------establish connections-------------------------------------------------

            var ibmc = new System.Data.Odbc.OdbcConnection(scs);
            var pgc = new NpgsqlConnection(dcs);
            ibmc.Open();
            pgc.Open();

            //----------------------------------------------setup commands---------------------------------------------------

            var ibmcmd = new System.Data.Odbc.OdbcCommand(sq,ibmc);
            var pgcom = pgc.CreateCommand();

            //---------------------------------------------begin transaction---------------------------------------------------------

            Console.Write(Environment.NewLine);
            Console.Write("etl start:" + DateTime.Now.ToString());
            NpgsqlTransaction pgt = pgc.BeginTransaction();
            ibmcmd.CommandTimeout = 600;
            System.Data.Odbc.OdbcDataReader ibmdr;
            try {
                ibmdr = ibmcmd.ExecuteReader();
            }
            catch (Exception e) {
                Console.Write(Environment.NewLine);
                Console.Write("error on source sql:");
                Console.Write(Environment.NewLine);
                Console.Write(e.Message);
                ibmc.Close();
                pgc.Close();
                return;
            }
            //setup getv object array dimensioned to number of columns for scenario
            var getv = new object[ibmdr.FieldCount];

            while (ibmdr.Read()) { 
                r = r + 1;
                t = t +1 ;
                nr = "";  
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
                                if (getv[i].ToString() == "1/1/0001 12:00:00 AM" || getv[i].ToString() == "") {
                                    nc = "NULL";
                                }
                                else {
                                    nc = "'" + getv[i].ToString() + "'";
                                }
                                break;
                            case "TIME":
                                nc = "'" + getv[i].ToString() + "'";
                                break;
                            case "TIMESTAMP":
                                nc = "'" + getv[i].ToString() + "'";
                                break;
                            default:
                                if (getv[i].ToString() != "") {
                                    nc = getv[i].ToString();
                                }
                                else {
                                    nc = "NULL";
                                }
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
                if (r == 250) {
                    r = 0;
                    sql = dt + sql;
                    pgcom.CommandText = sql;
                    try {
                        pgcom.ExecuteNonQuery();
                    }
                    catch (Exception e) {
                            Console.Write(Environment.NewLine);
                            Console.Write(e.Message);
                            ibmc.Close();
                            pgt.Rollback();
                            pgc.Close();
                    }
                    sql = "";
                }
            }
            if (r != 0) {
                sql = dt + sql;
                pgcom.CommandText = sql;
                try {
                    pgcom.ExecuteNonQuery();
                }
                catch (Exception e) {
                        Console.Write(Environment.NewLine);
                        Console.Write(e.Message);
                        //ibmc.Close();
                        pgt.Rollback();
                        pgc.Close();

                }
                sql = "";          
            }

            pgt.Commit();
            ibmc.Close();
            pgc.Close();

            Console.Write(Environment.NewLine);
            Console.Write("etl end:" + DateTime.Now.ToString());
            
        }
    }
}
