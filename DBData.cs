using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    public class DBData
    {
        private string db_name;
        private Dictionary<string, TableData> table_map = new Dictionary<string, TableData>();

        public DBData(string db_name)
        {
            this.db_name = db_name;
            string[] files = Directory.GetFiles(XMDB.DBPath(db_name), "*.data");
            foreach (string path in files)
            {
                string table_name = Path.GetFileNameWithoutExtension(path);
                this.LoadTable(table_name);
            }
        }

        public void Save()
        {
            foreach (string table_name in this.table_map.Keys)
            {
                this.SaveTable(table_name);
            }
        }

        private void LoadTable(string table_name)
        {
            FileStream stream = File.Open(this.TablePath(table_name), FileMode.Open);
            TableInfo table_info;
            if (XMDB.IsBinaryFormat)
            {
                table_info = (TableInfo)(new BinaryFormatter().Deserialize(stream));
            }
            else
            {
                StreamReader sr = new StreamReader(stream);
                string json = sr.ReadToEnd();
                sr.Close();
                table_info = JsonConvert.DeserializeObject<TableInfo>(json);
            }
            stream.Close();

            TableData table_data = new TableData(table_name);
            table_data.Load(table_info);
            this.table_map[table_name] = table_data;
        }

        private void SaveTable(string table_name)
        {
            TableData table_data = this.table_map[table_name];
            TableInfo table_info = table_data.Save();

            FileStream stream = File.Create(this.TablePath(table_name));
            if (XMDB.IsBinaryFormat)
            {
                new BinaryFormatter().Serialize(stream, table_info);
            }
            else
            {
                string json = JsonConvert.SerializeObject(table_info);
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                stream.Write(bytes, 0, bytes.Length);
            }
            stream.Close();
        }

        public void CreateTable(string table_name)
        {
            if (this.IsExistTable(table_name))
            {
                throw new Exception(string.Format("DB:{0} CreateTable Error | Exist Table:{1}", this.db_name, table_name));
            }
            File.Create(this.TablePath(table_name)).Close();
            this.table_map[table_name] = new TableData(table_name);
        }

        public void InitTable(string table_name)
        {
            if (!this.IsExistTable(table_name))
            {
                this.CreateTable(table_name);
            }
        }

        public Dictionary<string, string> TableInsert(string table_name, string json, string uid = "")
        {
            try
            {
                if (!this.IsExistTable(table_name))
                {
                    throw new Exception(string.Format("TableInsert DB:{0} Error | Not Exist Table:{1}", this.db_name, table_name));
                }
                return this.table_map[table_name].Insert(json, uid);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new Dictionary<string, string>();
            }
        }

        public Dictionary<string, string> TableDelete(string table_name, string condition)
        {
            try
            {
                if (!this.IsExistTable(table_name))
                {
                    throw new Exception(string.Format("TableDelete DB:{0} Error | Not Exist Table:{1}", this.db_name, table_name));
                }
                return this.table_map[table_name].Delete(condition);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new Dictionary<string, string>();
            }
        }

        public Dictionary<string, string> TableUpdate(string table_name, string condition, string json)
        {
            try
            {
                if (!this.IsExistTable(table_name))
                {
                    throw new Exception(string.Format("TableUpdate DB:{0} Error | Not Exist Table:{1}", this.db_name, table_name));
                }
                return this.table_map[table_name].Update(condition, json);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new Dictionary<string, string>();
            }
        }

        public Dictionary<string, string> TableSelect(string table_name, string condition)
        {
            try
            {
                if (!this.IsExistTable(table_name))
                {
                    throw new Exception(string.Format("TableSelect DB:{0} Error | Not Exist Table:{1}", this.db_name, table_name));
                }
                return this.table_map[table_name].Select(condition);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new Dictionary<string, string>();
            }
        }

        public void PrintTable(string table_name)
        {
            try
            {
                if (!this.IsExistTable(table_name))
                {
                    throw new Exception(string.Format("TableSelect DB:{0} Error | Not Exist Table:{1}", this.db_name, table_name));
                }
                Log.LogData(this.table_map[table_name].GetData(), "PrintTable");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private string TablePath(string table_name)
        {
            return string.Format("{0}/{1}/{2}.data", XMDB.RootPath, this.db_name, table_name);
        }

        private bool IsExistTable(string table_name)
        {
            return File.Exists(this.TablePath(table_name));
        }
    }
}
