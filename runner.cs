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
            var i = new Int32();

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
            //ibmcmd.CommandText = "SELECT * FROM RLARP.OSM WHERE ITER >= '2018-08-08-13.41.52.681140'";
            //ibmcmd.CommandText = "SELECT * FROM RLARP.OSMP";
            //ibmcmd.CommandText = "SELECT * FROM LGDAT.GLDATE";
            ibmcmd.CommandText = "SELECT * FROM LGDAT.STKMM";

            var pgcmd = new NpgsqlCommand();
            pgcmd.Connection = pgc;
            //pgcmd.CommandText = "SELECT * FROM rlarp.trig_log_eav WHERE 0=1";
            //pgcmd.CommandText = "SELECT * FROM rlarp.osmi WHERE 0=1";
            //pgcmd.CommandText = "SELECT * FROM rlarp.osmi WHERE 0=1";
            //pgcmd.CommandText = "SELECT * FROM lgdat.gldate WHERE 0=1";
            pgcmd.CommandText = "SELECT * FROM lgdat.stkmm WHERE 0=1";

            //---------------------------------------------setup adapters---------------------------------------------------------
            var ibmds = new System.Data.DataSet();
            var ibmda = new System.Data.Odbc.OdbcDataAdapter(ibmcmd);
            Console.Write(DateTime.Now);
            ibmda.Fill(ibmds);

            var pgds = new System.Data.DataSet();
            var pgda = new NpgsqlDataAdapter(pgcmd);
            pgda.Fill(pgds);

            //--------------------------------------------move to target--------------------------------------------------------
            foreach (System.Data.DataRow ibmr in ibmds.Tables[0].Rows) {
                var pgr = pgds.Tables[0].NewRow();
                pgr.ItemArray = ibmr.ItemArray;
                pgds.Tables[0].Rows.Add(pgr);
                i=i+1;
                if (i> 500){
                    new NpgsqlCommandBuilder(pgda);
                    pgda.Update(pgds);
                    i=0;
                }
            }
            new NpgsqlCommandBuilder(pgda);
            try {
                pgda.Update(pgds);
            }
            catch (Exception e) {
                //Console.WriteLine("{0} Exception caught.", e);
                Console.WriteLine(e.Message);
            }
            

            ibmc.Close();
            pgc.Close();

            Console.Write(DateTime.Now);
            
        }
    }
}
