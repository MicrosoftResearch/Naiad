/*
 * Naiad ver. 0.3
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace Microsoft.Research.Naiad.Util
{
    public class Flag
    {
        public readonly Type FlagType;
        private string _stringValue;
        public bool _boolValue;
        private int _intValue;
        private double _doubleValue;
        private List<string> _listValue = new List<string>();

        public Boolean IsSet = false;

        private static List<Type> KnownTypes = new Type[]
                                                   {
                                                       typeof (bool),
                                                       typeof (int), 
                                                       typeof (string), 
                                                       typeof (double),
                                                       typeof (List<String>)
                                                   }.ToList();

        private object _enumValue;

        public Flag(Type flagType)
        {
            FlagType = flagType;
            if (!FlagType.IsEnum && !KnownTypes.Contains(flagType))
            {
                throw new Exception("invalid flag type: " + flagType);
            }
        }

        public static implicit operator int(Flag f)
        {
            //Logging.Assert(f.FlagType == typeof(int), "tried to use a {0} flag as a {1}", f.FlagType, typeof(int));
            return f._intValue;
        }

        public static implicit operator bool(Flag f)
        {
            //Logging.Assert(f.FlagType == typeof(bool), "tried to use a {0} flag as a {1}", f.FlagType, typeof(bool));
            return f._boolValue;
        }

        public static implicit operator double(Flag f)
        {
            //Logging.Assert(f.FlagType == typeof(double), "tried to use a {0} flag as a {1}", f.FlagType, typeof(double));
            return f._doubleValue;
        }

        public static implicit operator string(Flag f)
        {
            //Logging.Assert(f.FlagType == typeof(string), "tried to use a {0} flag as a {1}", f.FlagType, typeof(string));
            return f._stringValue;
        }

        public static implicit operator List<string>(Flag f)
        {
            //Logging.Assert(f.FlagType == typeof(List<String>), "tried to use a {0} flag as a {1}", f.FlagType, typeof(List<String>));
            //Logging.Assert(f.FlagType == typeof(List<string>));
            return f._listValue;
        }

        public static implicit operator Flag(int v)
        {
            var f = new Flag(typeof(int));
            f.Parse(v.ToString());
            return f;
        }

        public static implicit operator Flag(List<string> v)
        {
            var f = new Flag(typeof(string));
            f.IsSet = true;
            f.Parse(String.Join(",", v));
            return f;
        }

        public IEnumerable<string> ListValue
        {
            get { return this._listValue; } // (List<string>)(this); }
        }

        public string StringValue
        {
            get { return this._stringValue; } // (String)(this); }
        }

        public int IntValue
        {
            get { return this._intValue; }// (int)(this); }
        }

        public T EnumValue<T>()
        {
            return (T)_enumValue;
        }

        public double DoubleValue
        {
            get { return this._doubleValue; }
        }

        public bool BooleanValue
        {
            get { return this._boolValue; }
        }

        public override string ToString()
        {
            return _stringValue;
        }

        public void Parse(string value)
        {
            // Check flag value is valid before assignment.
            IsSet = true;
            _stringValue = value;
            if (FlagType == typeof(bool))
            {
                _boolValue = Boolean.Parse(value);
            }
            else if (FlagType.IsEnum)
            {
                _enumValue = Enum.Parse(FlagType, _stringValue);
            }
            else if (FlagType == typeof(int))
            {
                _intValue = Int32.Parse(value);
            }
            else if (FlagType == typeof(double))
            {
                _doubleValue = Double.Parse(value);
            }
            else if (FlagType == typeof(List<string>))
            {
                _listValue.AddRange(value.Split(','));
            }
            else if (FlagType == typeof(string))
            {
                _stringValue = value;
            }
            else
            {
                throw new Exception("Invalid flag type: " + FlagType);
            }
        }

    }
    public static class Flags
    {
        private static Dictionary<string, Flag> _flagMap = new Dictionary<string, Flag>();

        public static Flag Define(string names, Type t)
        {
            var f = new Flag(t);
            foreach (var n in names.Split(','))
            {
                // Strip leading hyphens.
                var name = n;
                while (name.StartsWith("-"))
                {
                    name = name.Substring(1);
                }

                _flagMap[name] = f;
            }
            return f;
        }

        public static Flag Define(string name, string defValue)
        {
            var f = Define(name, defValue.GetType());
            f.Parse(defValue);
            return f;
        }

        public static Flag Define(string name, bool @default)
        {
            var f = Define(name, @default.GetType());
            f.Parse(@default.ToString());
            return f;
        }

        public static Flag Define(string name, int defValue)
        {
            var f = Define(name, defValue.GetType());
            f.Parse(defValue.ToString());
            return f;
        }

        public static Flag Define(string name, double defValue)
        {
            var f = Define(name, defValue.GetType());
            f.Parse(defValue.ToString());
            return f;
        }

        public static Flag Define<T>(string name, T @default) where T : struct
        {
            if (!typeof(T).IsEnum)
            {
                throw new ArgumentException("Only enum structs can be used as flags.");
            }
            var f = Define(name, typeof(T));
            f.Parse(@default.ToString());
            return f;
        }

        public static Flag Get(string name)
        {
            return _flagMap[name];
        }

        public static void Parse(NameValueCollection settings)
        {
            foreach (string key in settings.AllKeys)
            {
                if (_flagMap.ContainsKey(key))
                {
                    _flagMap[key].Parse(settings.Get(key));
                }
            }
        }

        public static String[] Parse(String[] args)
        {
            var unparsedArgs = new List<string>();
            var idx = 0;
            while (idx < args.Length)
            {
                var arg = args[idx];
                //Console.Error.WriteLine("Parsing... {0} {1}", idx, arg);

                ++idx;
                string name = null;
                if (arg.StartsWith("--"))
                {
                    name = arg.Substring(2);
                }
                else if (arg.StartsWith("-"))
                {
                    name = arg.Substring(1);
                }
                else
                {
                    //Console.Error.WriteLine("Skipping non-flag argument " + arg);
                    unparsedArgs.Add(arg);
                    continue;
                }

                String value = null;
                if (name.Contains("="))
                {
                    var parts = name.Split(new[] { '=' });
                    Debug.Assert(parts.Length == 2);
                    name = parts[0];
                    value = parts[1];
                }

                if (name == "helpflags")
                {
                    Console.Error.WriteLine("Application: {0}", Assembly.GetEntryAssembly().GetName());
                    Console.Error.WriteLine("Flags currently defined:");

                    foreach (var k in _flagMap.Keys.OrderBy(k => k))
                    {
                        var f = _flagMap[k];
                        Console.Error.WriteLine("  --{0} : {1} ({2})", k, f.FlagType.Name, f.ToString());
                    }
                    System.Environment.Exit(0);
                }

                if (!_flagMap.ContainsKey(name))
                {
                    //Console.Error.WriteLine("Skipping unknown argument " + name);
                    unparsedArgs.Add(arg);
                    continue;
                }

                var flag = _flagMap[name];

                if (flag.FlagType == typeof(bool))
                {
                    flag.Parse("true");
                }
                else
                {
                    if (value == null)
                    {
                        value = args[idx++];
                    }
                    flag.Parse(value);
                }
            }

            return unparsedArgs.ToArray();
        }
    }
}
