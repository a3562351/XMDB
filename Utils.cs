using Microsoft.CSharp;
using Newtonsoft.Json;
using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace XMDB
{
    internal class Utils
    {
        public static void LogData(Dictionary<string, string> data, string handle)
        {
            Console.WriteLine("-------------------------------------------");
            Console.WriteLine(string.Format("LogData Handle:{0}", handle));
            if (data.Keys.Count <= 0)
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

        public static string GetTypeStr(string str)
        {
            string param_type = "string";
            if (int.TryParse(str, out int result))
                param_type = "int";
            return param_type;
        }

        public static T CodeRun<T>(string param, string code, object[] value_list)
        {
            StringBuilder mStringBuilder = new StringBuilder();
            mStringBuilder.Append("using System;\n");
            mStringBuilder.Append("public class CodeRun\n");
            mStringBuilder.Append("{\n");
            mStringBuilder.Append("    public object run("+ param + ")\n");
            mStringBuilder.Append("    {\n");
            mStringBuilder.Append("        return " + code + ";\n");
            mStringBuilder.Append("    }\n");
            mStringBuilder.Append("}");
            //Console.WriteLine(mStringBuilder.ToString());

            CSharpCodeProvider mCSharpCodeProvider = new CSharpCodeProvider();
            CompilerParameters mCompilerParameters = new CompilerParameters();
            mCompilerParameters.ReferencedAssemblies.Add("System.dll");
            mCompilerParameters.GenerateExecutable = false;
            mCompilerParameters.GenerateInMemory = true;

            CompilerResults mCompilerResults = mCSharpCodeProvider.CompileAssemblyFromSource(mCompilerParameters, mStringBuilder.ToString());
            if (mCompilerResults.Errors.HasErrors)
            {
                Console.WriteLine("CodeRun CompilerResults Errors:");
                foreach (CompilerError err in mCompilerResults.Errors)
                {
                    Console.WriteLine(err.ErrorText);
                }
            }
            object asseval = mCompilerResults.CompiledAssembly.CreateInstance("CodeRun");
            MethodInfo method = asseval.GetType().GetMethod("run");
            object result = method.Invoke(asseval, value_list.Length > 0 ? value_list : null);
            GC.Collect();

            return (T)result;
        }
    }
}
