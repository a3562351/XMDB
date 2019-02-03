using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    public class XMDB
    {
        public static string RootPath;
        public static bool IsBinaryFormat = true;   //数据保存格式，true为二进制，false为Json
        public static Random RandomCreator = new Random(Guid.NewGuid().ToString().GetHashCode());

        public static void SetRoot(string path)
        {
            RootPath = InitPath(path);
            Console.WriteLine(RootPath);
        }

        public static void Create(string db_name)
        {
            if (IsExistDB(db_name))
            {
                throw new Exception(string.Format("DBCreate Error Exist DB:{0}", db_name));
            }
            string path = DBPath(db_name);
            InitPath(path);
        }

        public static DBData Connect(string db_name)
        {
            string path = DBPath(db_name);
            if (!Directory.Exists(path))
            {
                throw new Exception(string.Format("DBConnect Error Not Exist DB:{0}", db_name));
            }
            return new DBData(db_name);
        }

        public static DBData InitDB(string db_name)
        {
            if (!IsExistDB(db_name))
            {
                Create(db_name);
            }
            return Connect(db_name);
        }

        public static bool IsExistDB(string db_name)
        {
            return Directory.Exists(DBPath(db_name));
        }

        public static string DBPath(string db_name)
        {
            return string.Format("{0}/{1}/", RootPath, db_name);
        }

        private static string InitPath(string file_path)
        {
            string path = file_path.Substring(0, file_path.LastIndexOf("/"));
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            return file_path;
        }

        private static bool IsExistFile(string file_path)
        {
            string path = file_path.Substring(0, file_path.LastIndexOf("/"));
            return Directory.Exists(path) && File.Exists(file_path);
        }

        public static string CreateOperation(string value, string op = "==")
        {
            JObject jObject = new JObject();
            jObject["value"] = value;
            jObject["op"] = op;
            return jObject.ToString();
        }

        public static string CreateCondition(Dictionary<string, string> and_cond, Dictionary<string, string> or_cond = null)
        {
            JObject jObject = new JObject();
            foreach (KeyValuePair<string, string> pair in and_cond)
            {
                jObject[pair.Key] = pair.Value;
            }

            if(or_cond != null)
            {
                jObject["$or"] = new JObject();
                foreach (KeyValuePair<string, string> pair in or_cond)
                {
                    jObject["$or"][pair.Key] = pair.Value;
                }
            }
            return jObject.ToString();
        }
    }
}
