<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Main.aspx.cs" Inherits="FileSearch.Main" %>

<!DOCTYPE html>
<script type="text/javascript">
    function DownloadImage(control_id) {
        //alert(clicked_id);
        PageMethods.GetHBaseImageJS(control_id);
    }
</script>
<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
    <style type="text/css">
        #TextArea1 {
            width: 891px;
            height: 70px;
        }
    </style>
</head>
<body>
    <form id="form1" runat="server">
    <div>
                <asp:ScriptManager ID="ScriptManager1" runat="server" EnablePageMethods="true">
                </asp:ScriptManager>
                <asp:FileUpload ID="fuControl" runat="server" />
                <asp:Button ID="btnIndex" runat="server" OnClick="btnIndex_Click" Text="Index and Save File" />
                <br />
                <br />
                </div>
        <p>
            <asp:TextBox ID="txtSearch" runat="server" Width="210px"></asp:TextBox>
            <asp:Button ID="btnSearch" runat="server" OnClick="btnSearch_Click" Text="Search" />
        </p>
        <asp:UpdatePanel ID="UpdatePanel1" runat="server">
            <ContentTemplate> 
        <div id="adis" runat="server" style="font-family: Tahoma; font-size: small">
           </div>
                </ContentTemplate>
            </asp:UpdatePanel>
    </form>
</body>
</html>
