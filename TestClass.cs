using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    [TestFixture]
    public class TestClass
    {
        [Test]
        public void TestMethod()
        {
            XMDB.SetRoot(AppDomain.CurrentDomain.BaseDirectory + "/Test");
            XMDB.IsBinaryFormat = false;
            DBData db = XMDB.InitDB("TestDB");
            db.InitTable("TestTable");
            
            //Insert...
            JObject insert_jObject = new JObject();
            insert_jObject["TestKey"] = "TestValue";
            Dictionary<string, string> insert_data = db.TableInsert("TestTable", insert_jObject.ToString());
            Utils.LogData(insert_data, "Insert");

            //Update...
            JObject update_jObject = new JObject();
            update_jObject["TestKey"] = "TestValue1";
            Dictionary<string, string> update_and_cond = new Dictionary<string, string>();
            update_and_cond["TestKey"] = XMDB.CreateOperation("TestValue", "==");
            Dictionary<string, string> update_data = db.TableUpdate("TestTable", XMDB.CreateCondition(update_and_cond), update_jObject.ToString());
            Utils.LogData(update_data, "Update");

            //Select...
            Dictionary<string, string> select_and_cond = new Dictionary<string, string>();
            Dictionary<string, string> select_or_cond = new Dictionary<string, string>();
            select_and_cond["TestKey"] = XMDB.CreateOperation("TestValue1", "==");
            Dictionary<string, string> select_data = db.TableSelect("TestTable", XMDB.CreateCondition(select_and_cond, select_or_cond));
            Utils.LogData(select_data, "Select");

            //Delete...
            Dictionary<string, string> delete_and_cond = new Dictionary<string, string>();
            delete_and_cond["UID"] = XMDB.CreateOperation(insert_data.Keys.ToArray()[0], "==");
            Dictionary<string, string> delete_data = db.TableDelete("TestTable", XMDB.CreateCondition(delete_and_cond));
            Utils.LogData(delete_data, "Delete");

            db.Save();
            db.PrintTable("TestTable");
        }
    }
}
