using System;
using Npgsql;

namespace test
{
    class Program
    {
        private 

        static void Main(string[] args)
        {
            
            string scs = "";
            string dcs = "";
            string sdt = "";
            string ddt = "";
            string sq = "";
            string dt = "";
            Boolean trim = false;
            int r = 0;
            int t = 0;
            string sql = "";
            string nr = "";
            string nc = "";
            string nl = Environment.NewLine;
            //declaring these objects globally will asume these libraries exist, they may not
            System.Data.Odbc.OdbcConnection s_odbc = new System.Data.Odbc.OdbcConnection();
            System.Data.Odbc.OdbcConnection d_odbc = new System.Data.Odbc.OdbcConnection();
            NpgsqlConnection s_npgsql = new NpgsqlConnection();
            NpgsqlConnection d_npgsql = new NpgsqlConnection();
            System.Data.Common.DbConnection sc; //source connection
            System.Data.Common.DbConnection dc; //destination connection

            string msg = "Help:";
            msg = msg + nl + "version 0.25";
            msg = msg + nl;
            msg = msg + nl + "options:";
            msg = msg + nl + "-sdt       sourc driver type";
            msg = msg + nl + "-scs       source connection string";
            msg = msg + nl + "-ddt       destination driver type";
            msg = msg + nl + "-dcs       destination connection string";
            msg = msg + nl + "-sq        path to source query";
            msg = msg + nl + "-dt        fully qualified name of destination table";
            msg = msg + nl + "-t         trim text";
            msg = msg + nl + "--help     info";
            msg = msg + nl;
            msg = msg + nl + "available driver types:";
            msg = msg + nl + "-------------------------";
            msg = msg + nl + "* odbc";
            msg = msg + nl + "* npgsql";


            //---------------------------------------parse args into variables-------------------------------------------------

            for (int i = 0; i < args.Length; i = i +1 ){
                switch (args[i]) {
                    //sourc driver type
                    case "-sdt":
                        sdt = args[i+1];
                        break;
                    //source connection string
                    case "-scs":
                        scs = args[i+1];
                        break;
                    //destination driver type
                    case "-ddt":
                        ddt = args[i+1];
                        break;
                    //destination connection string
                    case "-dcs":
                        dcs = args[i+1];
                        break;
                    //source query path
                    case "-sq":
                        try {
                            sq = System.IO.File.ReadAllText(args[i+1]);
                        }
                        catch (Exception e) {
                            Console.Write(nl + "error reasing source sql file: " + e.Message);
                            return;
                        }
                        break;
                    //destination table name
                    case "-dt":
                        dt = "INSERT INTO " + args[i+1] + " VALUES ";
                        break;
                    case "-t":
                        trim = true;
                        break;
                    case "--help":
                        Console.Write(nl);
                        Console.Write(msg);
                        return;
                    case "-help":
                        Console.Write(nl);
                        Console.Write(msg);
                        return;
                    case "-h":
                        Console.Write(nl);
                        Console.Write(msg);
                        return;
                    case @"\?":
                        Console.Write(nl);
                        Console.Write(msg);
                        return;
                    default:
                        break;
                }
            }

            Console.Write(nl);
            Console.Write(sdt);
            Console.Write(scs);
            Console.Write(nl);
            Console.Write(ddt);
            Console.Write(dcs);
            Console.Write(nl);
            Console.Write(sq);
            Console.Write(nl);
            Console.Write(dt);

            //return;

            //-------------------------------------------establish connections-------------------------------------------------

            //setup source
            switch (sdt){
                case "odbc":
                    try {
                        s_odbc.ConnectionString = scs;
                    }
                    catch (Exception e) {
                        Console.Write(nl + "bad source connection string: " + e.Message);
                        return;
                    }
                    sc = s_odbc;
                    return;
                default:
                    break;
            }

            //setup destination
            switch (ddt) {
                case "npgsql":
                    try {
                        d_npgsql.ConnectionString = dcs;
                    }
                    catch (Exception e) {
                        Console.Write(nl + "bad source connection string: " + e.Message);
                        return;
                    }
                    dc = d_npgsql;
                    return;
                default:
                    break;
            }

            //polymorph open
            try {
                sc.Open();
            }
            catch (Exception e) {
                Console.Write(nl + "issue connection to source: " + e.Message);
                return;
            }

            try {
                dc.Open();
            }
            catch (Exception e) {
                sc.Close();
                Console.Write(nl + "issue connecting to destination: "+ e.Message);
            }
            
            

            //----------------------------------------------setup commands---------------------------------------------------

            var s_cmd = new System.Data.Odbc.OdbcCommand();
            var pgcom = d_npgsql.CreateCommand();

            //---------------------------------------------begin transaction---------------------------------------------------------

            Console.Write(nl);
            Console.Write("etl start:" + DateTime.Now.ToString());
            NpgsqlTransaction pgt = pgc.BeginTransaction();
            s_odbcommandTimeout = 6000;
            s_odbcommandTimeout = 6000;
            system.Data.Odbc.OdbcDataReader ibmdr;
            try {
                ibmdr = s_odbcxecuteReader();
                ibmdr = s_odbcxecuteReader();
    d    }
            catch (Exception e) {
                Console.Write(nl);
                Console.Write("error on source sql:");
                Console.Write(nl);
                Console.Write(e.Message);
                s_odbcse();
                s_odbcse();
                pgc.Cldse();
                return;
            }
            //setup getv object array dimensioned to number of columns for scenario
            //getv hold an array of datareader row
            var getv = new object[ibmdr.FieldCount];
            int cols = ibmdr.FieldCount;
            //dtn holds list of data types per column
            var dtn = new string[ibmdr.FieldCount];
            while (ibmdr.Read()) { 
                r = r + 1;
                t = t +1 ;
                nr = "";  
                //populate all the data type names into a string array instead of calling against ibmdr in every iteration
                if (t == 1 ) {
                    for (int i = 0; i < cols; i++){
                        dtn[i] = ibmdr.GetDataTypeName(i);
                    }
                }
                for (int i = 0 ; i < cols;i++) {
                    //Console.Write(nl);
                    //Console.Write(DBNull.Value.Equals(ibmdr.GetValue(i)).ToString());
                    //Console.Write(ibmdr.GetValue(i).ToString());
                    Boolean dnull = false; 
                    if (dtn[i] == "BIGINT") {
                        dnull = DBNull.Value.Equals(ibmdr.GetInt64(i));
                    } 
                    else {
                        dnull = DBNull.Value.Equals(ibmdr.GetValue(i));
                    }
                    if (!  dnull) {
                        switch (dtn[i]){
                            case "VARCHAR":
                                if (trim)  {
                                    nc = "'" + ibmdr.GetValue(i).ToString().Replace("'","''").TrimEnd() + "'";     
                                }
                                else {
                                    nc = "'" + ibmdr.GetValue(i).ToString().Replace("'","''") + "'";     
                                }
                                break;
                            case "CHAR":
                                if (trim)  {
                                    nc = "'" + ibmdr.GetValue(i).ToString().Replace("'","''").TrimEnd() + "'";     
                                }
                                else {
                                    nc = "'" + ibmdr.GetValue(i).ToString().Replace("'","''") + "'";     
                                }
                                break;
                            case "DATE":
                                if (ibmdr.GetValue(i).ToString() == "1/1/0001 12:00:00 AM" || ibmdr.GetValue(i).ToString() == "") {
                                    nc = "NULL";
                                }
                                else {
                                    nc = "'" + ibmdr.GetValue(i).ToString() + "'";
                                }
                                break;
                            case "TIME":
                                nc = "'" + ibmdr.GetValue(i).ToString() + "'";
                                break;
                            case "TIMESTAMP":
                                nc = "'" + ibmdr.GetDateTime(i).ToString() + "'";
                                break;
                            case "BIGINT":
                                nc = ibmdr.GetInt64(i).ToString();
                                break;
                            default:
                                if (ibmdr.GetValue(i).ToString() != "") {
                                    nc = ibmdr.GetValue(i).ToString();
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
                            Console.Write(nl);
                            Console.Write(e.Message);
                            System.IO.File.WriteAllText(@"C:\Users\ptrowbridge\Downloads\runner_error.sql",sql);
                            s_odbcse();
                            s_odbcse();
                            pgt.Rollbdck();
                            pgc.Close();
                            return;
                    }
                    sql = "";
                    Console.Write(nl + t.ToString());
                }
            }
            if (r != 0) {
                sql = dt + sql;
                pgcom.CommandText = sql;
                try {
                    pgcom.ExecuteNonQuery();
                }
                catch (Exception e) {
                        Console.Write(nl);
                        Console.Write(e.Message);
                        System.IO.File.WriteAllText(@"C:\Users\ptrowbridge\Downloads\runner_error.sql",sql);
                        //s_odbcse();
                        //s_odbcse();
                        pgt.Rodlback();
                        pgc.Close();
                        return;
                }
                sql = "";    
                Console.Write(nl + t.ToString());      
            }

            pgt.Commit();
            s_odbcse();
            s_odbcse();
            pgc.Cldse();

            Console.Write(nl);
            Console.Write("etl end:" + DateTime.Now.ToString());
            
        }
    }
}
