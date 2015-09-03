using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Thrift.Transport;
using Thrift.Protocol;
using System.Text;

namespace FileSearch
{
    public class CallHBase
    {
        string _documentId;
        internal const string _hbaseThrif = "172.24.2.144";
        internal const int _hbaseThrifPort = 9090;
        public CallHBase(string documentId)
        {
            _documentId = documentId;
        }

        public Dictionary<byte[], TCell> GetHBaseImage()
        {
            var socket = new TSocket(_hbaseThrif, _hbaseThrifPort);
            var transport = new TBufferedTransport(socket);
            var protocol = new TBinaryProtocol(transport);
            Hbase.Client hbaseClient = new Hbase.Client(protocol);
            Dictionary<byte[], TCell> hbaseResult = new Dictionary<byte[], TCell>();
            transport.Open();

            //List<byte[]> tableNames = hc.getTableNames();

            byte[] table = Encoding.UTF8.GetBytes("FiservImages");
            byte[] row = Encoding.UTF8.GetBytes(_documentId);
            byte[] column = Encoding.UTF8.GetBytes("ImageData:Image");

            List<TRowResult> results = hbaseClient.getRow(table, row, null);
            TCell t = new TCell();
            //t.

            foreach (TRowResult result in results)
            {
                //result.Columns[column]
                //result.Columns;
                
                hbaseResult = result.Columns;

                //string s = Encoding.UTF8.GetString(result.Columns[column].Value);

                //foreach (KeyValuePair<byte[], TCell> singleRow in konj)
                //{
                //    //File.WriteAllBytes(@"c:\test\output.docx", singleRow.Value.Value);
                //    Main a = new Main();
                //    a.OpenFile(singleRow.Value.Value, "output.docx");
                //}
            }
            return hbaseResult;
        }
    }
}