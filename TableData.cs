using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    [Serializable]
    internal class RowData : Dictionary<string, string> {
        public RowData() { }
        public RowData(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
    [Serializable]
    internal class RowMap : Dictionary<string, RowData> {
        public RowMap() { }
        public RowMap(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
    internal class IndexData : Dictionary<string, List<string>> { }
    internal class IndexMap : Dictionary<string, IndexData> { }

    [Serializable]
    internal class TableInfo
    {
        public RowMap DataMap;
        public List<string> Index;
    }

    internal class TableData
    {
        private string table_name;
        private RowMap row_map = new RowMap();
        private IndexMap index_map = new IndexMap();

        public TableData(string table_name)
        {
            this.table_name = table_name;
        }

        public void Load(TableInfo table_info)
        {
            this.row_map = table_info.DataMap;

            foreach (string index in table_info.Index)
            {
                this.index_map[index] = new IndexData();
            }

            this.ReflushIndex();
        }

        public TableInfo Save()
        {
            TableInfo table_info = new TableInfo();
            table_info.DataMap = this.row_map;
            table_info.Index = this.index_map.Keys.ToList();
            return table_info;
        }

        public Dictionary<string, string> GetData()
        {
            Dictionary<string, string> data = new Dictionary<string, string>();
            foreach (KeyValuePair<string, RowData> pair in this.row_map)
            {
                data[pair.Key] = this.RowDataToJson(pair.Value);
            }
            return data;
        }

        public Dictionary<string, string> Insert(string json, string uid)
        {
            if (!uid.Equals("") && this.row_map.ContainsKey(uid))
            {
                throw new Exception(string.Format("Insert Table:{0} Error | Exist Uid:{1}", this.table_name, uid));
            }
            uid = !uid.Equals("") ? uid : this.CreateUID();
            this.row_map.Add(uid, this.JsonToRowData(json));
            this.OnAddRowData(uid, this.row_map[uid]);
            Dictionary<string, string> data = new Dictionary<string, string>();
            data[uid] = this.RowDataToJson(this.row_map[uid]);
            return data;
        }

        public Dictionary<string, string> Delete(string condition)
        {
            RowMap search_row_map = this.Filter(condition, "Delete", true);
            Dictionary<string, string> data = new Dictionary<string, string>();
            foreach (KeyValuePair<string, RowData> pair in search_row_map)
            {
                this.row_map.Remove(pair.Key);
                this.OnRemoveRowData(pair.Key, pair.Value);
                data[pair.Key] = this.RowDataToJson(pair.Value);
            }
            return data;
        }

        public Dictionary<string, string> Update(string condition, string json)
        {
            RowMap search_row_map = this.Filter(condition, "Update", true);
            Dictionary<string, string> data = new Dictionary<string, string>();
            foreach (KeyValuePair<string, RowData> pair in search_row_map)
            {
                this.OnRemoveRowData(pair.Key, this.row_map[pair.Key]);
                this.row_map[pair.Key] = this.JsonToRowData(json);
                this.OnAddRowData(pair.Key, this.row_map[pair.Key]);
                data[pair.Key] = this.RowDataToJson(this.row_map[pair.Key]);
            }
            return data;
        }

        public Dictionary<string, string> Select(string condition)
        {
            RowMap search_row_map = this.Filter(condition, "Select", false);
            Dictionary<string, string> data = new Dictionary<string, string>();
            foreach (KeyValuePair<string, RowData> pair in search_row_map)
            {
                data[pair.Key] = this.RowDataToJson(pair.Value);
            }
            return data;
        }

        public void AddIndex(params string[] index_list)
        {
            bool need_reflush = false;
            foreach (string index in index_list)
            {
                if (!this.index_map.ContainsKey(index))
                {
                    this.index_map[index] = new IndexData();
                    need_reflush = true;
                }
            }

            if (need_reflush)
            {
                this.ReflushIndex();
            }
        }

        private int GetCurTime()
        {
            return (int)(DateTime.Now - TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1))).TotalSeconds;
        }

        //4字节：时间戳 
        //4字节：表示生成此UID的进程 
        //4字节：由一个随机数开始的计数器生成的值
        private string CreateUID()
        {
            int random = XMDB.RandomCreator.Next();
            return this.GetCurTime().ToString("X") + Process.GetCurrentProcess().Id.ToString("X") + random.ToString("X");
        }

        private void ReflushIndex()
        {
            foreach (IndexData index_data in this.index_map.Values)
            {
                index_data.Clear();
            }

            foreach (KeyValuePair<string, RowData> pair in this.row_map)
            {
                this.OnAddRowData(pair.Key, pair.Value);
            }
        }

        private void OnAddRowData(string uid, RowData row_data)
        {
            foreach (KeyValuePair<string, string> pair in row_data)
            {
                if (this.index_map.ContainsKey(pair.Key))
                {
                    if (!this.index_map[pair.Key].ContainsKey(pair.Value))
                    {
                        this.index_map[pair.Key][pair.Value] = new List<string>();
                    }
                    this.index_map[pair.Key][pair.Value].Add(uid);
                }
            }
        }

        private void OnRemoveRowData(string uid, RowData row_data)
        {
            foreach (KeyValuePair<string, string> pair in row_data)
            {
                if (this.index_map.ContainsKey(pair.Key))
                {
                    if (this.index_map[pair.Key].ContainsKey(pair.Value))
                    {
                        this.index_map[pair.Key][pair.Value].Remove(uid);
                    }
                }
            }
        }

        private RowData JsonToRowData(string json)
        {
            RowData data = new RowData();
            JObject jObject = (JObject)JsonConvert.DeserializeObject(json);
            foreach (KeyValuePair<string, JToken> pair in jObject)
            {
                data[pair.Key] = pair.Value.ToString();
            }
            return data;
        }

        private string RowDataToJson(RowData data)
        {
            JObject jObject = new JObject();
            foreach (KeyValuePair<string, string> pair in data)
            {
                jObject[pair.Key] = pair.Value;
            }
            return jObject.ToString();
        }

        private RowMap Filter(string condition, string handle, bool is_warn = false)
        {
            JObject cond = (JObject)JsonConvert.DeserializeObject(condition);

            RowMap search_row_map = new RowMap();
            //有指定UID先筛选
            if (cond.ContainsKey("UID"))
            {
                JObject target = (JObject)JsonConvert.DeserializeObject(cond["UID"].ToString());
                if (target["op"].ToString().Equals("=="))
                {
                    string uid = target["value"].ToString();
                    if (!this.row_map.ContainsKey(uid))
                    {
                        if (is_warn)
                        {
                            throw new Exception(string.Format("{0} Table:{1} Error | Not Exist Uid:{2}", handle, this.table_name, uid));
                        }
                        return search_row_map;
                    }
                    search_row_map[uid] = this.row_map[uid];
                }
                else
                {
                    search_row_map = this.row_map;
                }
            }
            else
            {
                search_row_map = this.row_map;
            }

            //筛选条件先与后或
            if(cond.Count > (cond.ContainsKey("$or") ? 1 : 0))
                search_row_map = this.FilterCondition(search_row_map, cond, true);

            if (cond.ContainsKey("$or") && ((JObject)cond["$or"]).Count > 0)
                search_row_map = this.FilterCondition(search_row_map, (JObject)cond["$or"], false);

            return search_row_map;
        }

        private RowMap FilterCondition(RowMap search_row_map, JObject cond, bool and_cond)
        {
            bool need_check = false;

            //先用索引筛选一遍
            RowMap and_filter_map = search_row_map;
            RowMap or_filter_map = new RowMap();
            foreach (KeyValuePair<string, JToken> pair in cond)
            {
                if (!pair.Key.Equals("$or"))
                {
                    string value = pair.Value.ToString();
                    if (this.index_map.ContainsKey(pair.Key))
                    {
                        if (!this.index_map[pair.Key].ContainsKey(value))
                        {
                            //与条件筛选出的数据为空
                            if (and_cond)
                            {
                                return new RowMap();
                            }
                        }
                        else
                        {
                            RowMap filter_row_map = new RowMap();
                            foreach (string uid in this.index_map[pair.Key][value])
                            {
                                if (search_row_map.ContainsKey(uid))
                                {
                                    if (and_cond)
                                    {
                                        if (and_filter_map.ContainsKey(uid))
                                        {
                                            filter_row_map[uid] = this.row_map[uid];
                                        }
                                    }
                                    else
                                    {
                                        or_filter_map[uid] = this.row_map[uid];
                                    }
                                }
                            }
                            and_filter_map = filter_row_map;
                        }
                    }
                    else
                    {
                        need_check = true;
                    }
                }
            }

            //存在没有索引的条件，需要遍历判断
            if (need_check)
            {
                RowMap filter_row_map = new RowMap();
                foreach (KeyValuePair<string, RowData> pair in search_row_map)
                {
                    string uid = pair.Key;
                    if (this.CheckCondition(uid, pair.Value, cond, and_cond))
                    {
                        filter_row_map[uid] = this.row_map[uid];
                    }
                }
                search_row_map = filter_row_map;
            }
            return search_row_map;
        }

        private bool CheckCondition(string uid, RowData row_data, JObject cond, bool and_cond)
        {
            foreach (KeyValuePair<string, JToken> pair in cond)
            {
                if (!pair.Key.Equals("$or"))
                {
                    JObject target = (JObject)JsonConvert.DeserializeObject(pair.Value.ToString());
                    string target_value = target["value"].ToString();
                    string op = target["op"].ToString();
                    bool check = pair.Key.Equals("UID") ? this.CheckMatch(uid, target["value"].ToString(), target["op"].ToString()) : (row_data.ContainsKey(pair.Key) && this.CheckMatch(row_data[pair.Key], target_value, op));
                    if (check)
                    {
                        if (!and_cond)
                        {
                            return true;
                        }
                    }
                    else
                    {
                        if (and_cond)
                        {
                            return false;
                        }
                    }
                }
            }
            return and_cond ? true : false;
        }

        private bool CheckMatch(string value, string target_value, string op)
        {
            string param = string.Format("object value, object target_value");
            string code = string.Format("({0})value {1} ({2})target_value", Utils.GetTypeStr(value), op, Utils.GetTypeStr(target_value));
            return Utils.CodeRun<bool>(param, code, new object[] { value, target_value });
        }
    }
}
