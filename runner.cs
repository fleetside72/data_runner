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
            string sq = "";
            string dt = "";
            Boolean trim = false;
            int r = 0;
            int t = 0;
            string sql = "";
            string nr = "";
            string nc = "";
            string nl = Environment.NewLine;
            var ibmc;
            var pgc;

            string msg = "Help:";
            msg = msg + nl + "version 0.22";
            msg = msg + nl + "-scs       source connection string";
            msg = msg + nl + "-dcs       destination connection string";
            msg = msg + nl + "-sq        path to source query";
            msg = msg + nl + "-dt        fully qualified name of destination table";
            msg = msg + nl + "-t         trim text";
            msg = msg + nl + "--help     info";


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
            Console.Write(scs);
            Console.Write(nl);
            Console.Write(dcs);
            Console.Write(nl);
            Console.Write(sq);
            Console.Write(nl);
            Console.Write(dt);

            //return;

            //-------------------------------------------establish connections-------------------------------------------------

            try {
                ibmc = new System.Data.Odbc.OdbcConnection(scs);
            }
            catch (Exception e) {
                Console.Write(nl + "bad source connection string: " + e.Message);
            }
            try {
                pgc = new NpgsqlConnection(dcs);
            }
            catch (Exception e) {
                Console.Write(nl + "bad source connection string: " + e.Message);
            }
            try {
                ibmc.Open();
            }
            catch (Exception e) {
                Console.Write(nl + "issue connection to source: " + e.Message);
                return;
            }

            try {
                pgc.Open();
            }
            catch (Exception e) {
                ibmc.Close();
                Console.Write(nl + "issue connecting to destination: "+ e.Message);
            }
            
            

            //----------------------------------------------setup commands---------------------------------------------------

            var ibmcmd = new System.Data.Odbc.OdbcCommand(sq,ibmc);
            var pgcom = pgc.CreateCommand();

            //---------------------------------------------begin transaction---------------------------------------------------------

            Console.Write(nl);
            Console.Write("etl start:" + DateTime.Now.ToString());
            NpgsqlTransaction pgt = pgc.BeginTransaction();
            ibmcmd.CommandTimeout = 6000;
            System.Data.Odbc.OdbcDataReader ibmdr;
            try {
                ibmdr = ibmcmd.ExecuteReader();
            }
            catch (Exception e) {
                Console.Write(nl);
                Console.Write("error on source sql:");
                Console.Write(nl);
                Console.Write(e.Message);
                ibmc.Close();
                pgc.Close();
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
                            ibmc.Close();
                            pgt.Rollback();
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
                        //ibmc.Close();
                        pgt.Rollback();
                        pgc.Close();
                        return;
                }
                sql = "";    
                Console.Write(nl + t.ToString());      
            }

            pgt.Commit();
            ibmc.Close();
            pgc.Close();

            Console.Write(nl);
            Console.Write("etl end:" + DateTime.Now.ToString());
            
        }
    }
}
