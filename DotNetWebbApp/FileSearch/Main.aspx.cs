using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using SolrNet;
using Microsoft.Practices.ServiceLocation;
using System.IO;
using SolrNet.Commands.Parameters;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Data.Odbc;
using System.Data;
using Thrift.Transport;
using Thrift.Protocol;
using System.Web.Services;
using System.Web.Script.Services;

namespace FileSearch
{
    public partial class Main : System.Web.UI.Page
    {
        internal const string hbaseThrif = "172.24.2.144";
        internal const int hbaseThrifPort = 9090;

        protected void Page_Load(object sender, EventArgs e)
        {
            //GetHBaseImage();
        }

        protected void HBaseMutateImage(string rowKey, byte[] image)
        {
            var socket = new TSocket("172.24.2.144", 9090);
            var transport = new TBufferedTransport(socket);
            var protocol = new TBinaryProtocol(transport);
            Hbase.Client hc = new Hbase.Client(protocol);
            transport.Open();

            //List<byte[]> tableNames = hc.getTableNames();

            byte[] table = Encoding.UTF8.GetBytes("FiservImages");
            byte[] row = Encoding.UTF8.GetBytes(rowKey);
            byte[] column = Encoding.UTF8.GetBytes("ImageData:Image");

            List<Mutation> mutation = new List<Mutation>();
            Mutation m = new Mutation();
            m.Column = column;
            m.Value = image;
            m.IsDelete = false;
            m.WriteToWAL = true;
            mutation.Add(m);

            hc.mutateRow(table, row, mutation, null);
        }

        protected void btnIndex_Click(object sender, EventArgs e)
        {
            if (fuControl.HasFile)
            {
                var solr = ServiceLocator.Current.GetInstance<ISolrOperations<FiservFileCollection>>();
                Byte[] fileBytes = fuControl.FileBytes;
                string uuid = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff");
                using (MemoryStream fileStream = new MemoryStream(fileBytes))
                {
                    var response = solr.Extract(new ExtractParameters(fileStream, uuid, fuControl.FileName)
                    {
                        ExtractOnly = false,
                        ExtractFormat = ExtractFormat.Text
                        
                    });
                    solr.Commit();
                    HBaseMutateImage(uuid, fileBytes);
                }
            }
        }

        protected void QueryWithHighlighting(string[] searchText)
        {
            var solr = ServiceLocator.Current.GetInstance<ISolrOperations<FiservFileCollection>>();
            var results = solr.Query(new SolrQueryInList("text", searchText), new QueryOptions
            {
                Highlight = new HighlightingParameters
                {
                     Fields = new[] { "content" },
                     Snippets = 5,
                     Fragsize = 120,
                     MaxAnalyzedChars = 100000,
                     BeforeTerm = "<span style=\"background-color:lime;\">",
                     AfterTerm = "</span>"
                }
            });

            var returnedResults = new List<Dictionary<string, string>>();

            if (results.Count > 0)
            {
                int count = 0;
                foreach (var pdfDoc in results)
                {
                    var docBuffer = new Dictionary<string, string>();
                    docBuffer.Add("Id", "" + pdfDoc.Id);
                    docBuffer.Add("Content", "" + pdfDoc.content);
                    docBuffer.Add("Author", pdfDoc.author);
                    docBuffer.Add("Resourcename", pdfDoc.resourcename);
                    
                    foreach (var h in results.Highlights[results[count].Id])
                    {
                        docBuffer.Add("SearchSnippet", String.Format("{0}", string.Join("<br /> ", h.Value.ToArray())));
                    }
                    returnedResults.Add(docBuffer);
                    count++;
                }
            }

            StringBuilder sb = new StringBuilder();
            foreach (Dictionary<string, string> search in returnedResults)
            {
                //Response.Write(" DocumentId " + search["Id"] + ": " + search["SearchSnippet"]);
                sb.Append("<input id=\"" 
                    + search["Id"] + "\" type=\"button\" value=\"Open Doc\" onclick=\"DownloadImage(this.id)\" />" 
                    + "   <b>DocumentId:</b> " + search["Id"] 
                    + "   <b>Author:</b> " + (String.IsNullOrEmpty(search["Author"]) ? "N/A" : search["Author"]) 
                    + "   <b>File</b> :" + search["Resourcename"] + "<br /><br />" + search["SearchSnippet"] + "<br /><br />");
            }
            adis.InnerHtml = sb.ToString();
        }

        public void GetHBaseImage(string rowKey = "20150822180801118")
        {
            var socket = new TSocket(hbaseThrif, hbaseThrifPort);
            var transport = new TBufferedTransport(socket);
            var protocol = new TBinaryProtocol(transport);
            Hbase.Client hbaseClient = new Hbase.Client(protocol);
            transport.Open();

            //List<byte[]> tableNames = hc.getTableNames();

            byte[] table = Encoding.UTF8.GetBytes("FiservImages");
            byte[] row = Encoding.UTF8.GetBytes(rowKey);
            byte[] column = Encoding.UTF8.GetBytes("ImageData:Image");

            List<TRowResult> results = hbaseClient.getRow(table, row, null);
            TCell t = new TCell();
            //t.

            foreach (TRowResult result in results)
            {
                //result.Columns[column]
                //result.Columns;
                Dictionary<byte[], TCell> konj;
                konj = result.Columns;

                //string s = Encoding.UTF8.GetString(result.Columns[column].Value);

                foreach (KeyValuePair<byte[], TCell> singleRow in konj)
                {
                    //File.WriteAllBytes(@"c:\test\output.docx", singleRow.Value.Value);
                    OpenFile(singleRow.Value.Value, "output.docx");
                }
            }
        }

        protected void btnSearch_Click(object sender, EventArgs e)
        {
            QueryWithHighlighting(txtSearch.Text.Split(' '));
        }

        public void OpenFile(byte[] file, string Name)
        {
            int fileSize = file.Length;

            Response.AppendHeader("content-length", fileSize.ToString());
            Response.ContentType = "octet-stream";
            if (!Name.Contains("."))
                Name = Name + ".(your_extension)";
            Response.AddHeader("Content-Disposition", "attachment; filename=" + Name);
            Response.BinaryWrite(file);
            Response.Flush();
            Response.End();
        }

        public static String QuerySolrFile(string documentId)
        {
            var solr = ServiceLocator.Current.GetInstance<ISolrOperations<FiservFileCollection>>();
            var docs = solr.Query(new SolrQueryByField("id", documentId));

            return Path.GetFileName(docs[0].resourcename);
        }

        [WebMethod]
        public static void GetHBaseImageJS(string id)
        {
            CallHBase c = new CallHBase(id.Trim());
            Dictionary<byte[], TCell> konj = c.GetHBaseImage();

            byte[] file = new byte[600000];

            foreach (KeyValuePair<byte[], TCell> singleRow in konj)
            {
                //File.WriteAllBytes(@"c:\test\output.docx", singleRow.Value.Value);
                file = singleRow.Value.Value;
            }
            
            int fileSize = file.Length;
            String Name = QuerySolrFile(id);

            string strdocPath;
            strdocPath = @"C:\Users\HDP\Downloads\" + Name;
            FileStream objfilestream = new FileStream(strdocPath, FileMode.Create, FileAccess.ReadWrite);
            objfilestream.Write(file, 0, file.Length);
            objfilestream.Close();
            System.Diagnostics.Process.Start(@"C:\Users\HDP\Downloads\" + Name);

        }
    }
}