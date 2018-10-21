using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    internal class Log
    {
        public static void LogData(Dictionary<string, string> data, string handle)
        {
            Console.WriteLine("-------------------------------------------");
            Console.WriteLine(string.Format("LogData Handle:{0}", handle));
            if(data.Keys.Count <= 0)
            {
                Console.WriteLine("{}");
            }
            else
            {
                foreach (KeyValuePair<string, string> pair in data)
                {
                    Console.WriteLine(JsonConvert.SerializeObject(pair));
                }
            }
            Console.WriteLine("-------------------------------------------");
        }
    }
}
