/*
 * Naiad ver. 0.2
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

﻿using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.CSharp;

namespace Naiad.CodeGeneration
{
    public enum IntSerialization
    {
        FixedLength,
        VariableLength
    }

    [System.AttributeUsage(AttributeTargets.Field)]
    public class IntSerializationAttribute : Attribute
    {
        public readonly IntSerialization Mode;

        public IntSerializationAttribute(IntSerialization mode)
        {
            this.Mode = mode;
        }
    }

    public interface NaiadSerialization<T> : IEqualityComparer<T>
    {
        bool Serialize(ref SubArray<byte> destination, T value);
        bool TryDeserialize(ref RecvBuffer source, out T value);
    }

    public static class PrimitiveSerializers
    {
        private static NaiadSerialization<byte> _byte = null;
        public static NaiadSerialization<byte> Byte
        {
            get
            {
                if (PrimitiveSerializers._byte == null)
                    PrimitiveSerializers._byte = AutoSerialization.GetSerializer<byte>();
                return PrimitiveSerializers._byte;
            }
        }

        private static NaiadSerialization<int> int32 = null;
        public static NaiadSerialization<int> Int32
        {
            get
            {
                if (PrimitiveSerializers.int32 == null)
                    PrimitiveSerializers.int32 = AutoSerialization.GetSerializer<int>();
                return PrimitiveSerializers.int32;
            }
        }

        private static NaiadSerialization<Int64> int64 = null;
        public static NaiadSerialization<Int64> Int64
        {
            get
            {
                if (PrimitiveSerializers.int64 == null)
                    PrimitiveSerializers.int64 = AutoSerialization.GetSerializer<Int64>();
                return PrimitiveSerializers.int64;
            }
        }

        private static NaiadSerialization<bool> _bool = null;
        public static NaiadSerialization<bool> Bool
        {
            get
            {
                if (PrimitiveSerializers._bool == null)
                    PrimitiveSerializers._bool = AutoSerialization.GetSerializer<bool>();
                return PrimitiveSerializers._bool;
            }
        }
    }

    internal class CompileCache
    {
        public string SerializationDir;
        public string SourceDir;
        public string ObjectDir;
        public string CacheDir;

        internal CompileCache(string dir)
        {
            SerializationDir = dir;
#if DEBUG
            CacheDir = SerializationDir + "/DebugCache";
            ObjectDir = SerializationDir + "/DebugObjects";
            SourceDir = SerializationDir + "/DebugSource";
#else
            CacheDir = SerializationDir + "/ReleaseCache";
            ObjectDir = SerializationDir + "/ReleaseObjects";
            SourceDir = SerializationDir + "/ReleaseSource";
#endif
            Directory.CreateDirectory(SourceDir);
            Directory.CreateDirectory(ObjectDir);
            Directory.CreateDirectory(CacheDir);

            Logging.Info("Cleaning up old generated files...");
            foreach (var f in Directory.EnumerateFiles(ObjectDir))
            {
                File.Delete(f);
            }
        }

        public void Store(string genSource, string pathToDll)
        {
            var pathToPdb = Path.ChangeExtension(pathToDll, ".pdb");
            var cacheName = CacheDir + "/cache-" + genSource.GetHashCode().ToString();
            
            var copied = false;
            while (!copied)
            {
                try
                {
                    if (File.Exists(pathToPdb))
                        File.Copy(pathToPdb, cacheName + ".pdb", true);

                    File.Copy(pathToDll, cacheName + ".dll", true);
                    copied = true;
                }
                catch
                {
                    System.Threading.Thread.Sleep(100);
                }
            }
        }

        public Assembly Load(string genSource)
        {
            var dllName = CacheDir + "/cache-" + genSource.GetHashCode().ToString() + ".dll";
            if (File.Exists(dllName))
            {
                return Assembly.LoadFile(dllName);
            }
            return null;
        }
    }

#if DEBUG
    public
#else
    public
#endif
        static class AutoSerialization
    {
        private static readonly Regex BadClassChars = new Regex("[^A-Za-z0-9_]");

        private static object _codegenLock = new object();

        private static CompileCache _compileCache = new CompileCache(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
        private static Dictionary<Type, object> _codeGenCache = new Dictionary<Type, object>();

        public static bool Serialize<T>(ref SubArray<byte> destination, T value)
        {
            return GetSerializer<T>().Serialize(ref destination, value);
        }

        public static bool TryDeserialize<T>(ref RecvBuffer source, out T value)
        {
            return GetSerializer<T>().TryDeserialize(ref source, out value);
        }

        public static NaiadSerialization<T> GetSerializer<T>(T dummy)
        {
            return GetSerializer<T>();
        }

        public static string FileNameForType(Type t)
        {
            var className = t.Name;
            foreach (var gt in t.GetGenericArguments())
                className = className + "-" + gt.Name;
            return className;
        }

        public static string ClassNameForCompile(Type t)
        {
            using (var codeProvider = new CSharpCodeProvider())
            {
                using (var sourceStream = new StringWriter())
                {
                    var codeGenOpts = new CodeGeneratorOptions();
                    codeProvider.GenerateCodeFromExpression(new CodeTypeReferenceExpression(t), sourceStream, codeGenOpts);
                    return sourceStream.ToString();
                }
            }
        }

        public static NaiadSerialization<T> GetSerializer<T>()
        {
            lock (_codegenLock)
            {
                Type t = typeof(T);

                if (!_codeGenCache.ContainsKey(t))
                {
                    var gen = new NaiadSerializationCodeGenerator(t);
                    object serializer = CompileSerializationCode(gen);
                    _codeGenCache[t] = serializer;
                }

                return (NaiadSerialization<T>)_codeGenCache[t];
            }
        }

        private static object CompileSerializationCode(NaiadSerializationCodeGenerator codeGen)
        {
            Tracing.Trace("[Compile");
            CodeCompileUnit compileUnit = codeGen.CompileUnit;

            using (var codeProvider = new CSharpCodeProvider())
            {
                using (var sourceStream = new StringWriter())
                {
                    var codeGenOpts = new CodeGeneratorOptions();
                    codeGenOpts.BlankLinesBetweenMembers = true;
                    codeGenOpts.ElseOnClosing = true;
                    codeGenOpts.BracingStyle = "Block";
                    codeGenOpts.IndentString = "  ";


                    codeProvider.GenerateCodeFromCompileUnit(compileUnit, sourceStream, codeGenOpts);

                    var sourceData = sourceStream.ToString();

                    //_compileCache.Lock();

                    var cachedAssembly = _compileCache.Load(sourceData);
                    if (cachedAssembly != null)
                    {
                        // Logging.Info("Found cached assembly for {0}", codeGen.CodeGenClass);
                        return cachedAssembly.CreateInstance(String.Format("Naiad.AutoGenerated.{0}", codeGen.CodeGenClass));
                    }

                    var sourcePath = FileNameForType(codeGen.Type);
                    sourcePath = BadClassChars.Replace(sourcePath, "_");
                    sourcePath = Path.Combine(_compileCache.SourceDir, sourcePath + ".cs");
                    sourcePath = Path.GetFullPath(sourcePath);

                    // Delete any pre-existing source file.
                    File.Delete(sourcePath);

                    File.WriteAllText(sourcePath, sourceData);

                    Logging.Info("Compiling codegen for {0}", codeGen.CodeGenClass);

                    var compilerOpts = new CompilerParameters();
                    compilerOpts.GenerateInMemory = false;
#if DEBUG
            compilerOpts.CompilerOptions = "/debug";
#else
                    compilerOpts.CompilerOptions = "/optimize+";
                    compilerOpts.TreatWarningsAsErrors = false;
#endif
                    compilerOpts.IncludeDebugInformation = true;

                    compilerOpts.ReferencedAssemblies.Add("System.dll");
                    compilerOpts.ReferencedAssemblies.Add("System.Core.dll");

                    foreach (var assembly in codeGen.GeneratedTypes.Select(Assembly.GetAssembly).Distinct())
                        compilerOpts.ReferencedAssemblies.Add(assembly.Location);
                    
                    compilerOpts.ReferencedAssemblies.Add(typeof(AutoSerialization).Assembly.Location);
                    
                    compilerOpts.GenerateInMemory = false; //default

                    compilerOpts.TempFiles = new TempFileCollection(_compileCache.ObjectDir, true);
                    compilerOpts.IncludeDebugInformation = true;
                    compilerOpts.TempFiles.KeepFiles = true;

                    //var res = codeProvider.CompileAssemblyFromFile(compilerOpts, sourcePath);
                    var res = codeProvider.CompileAssemblyFromSource(compilerOpts, new string[] { sourceData });

                    if (res.Errors.Count > 0)
                    {
                        var errorMsg = res.Errors.Cast<CompilerError>().Aggregate("", (current, ce) => current + (ce + "\n"));
                        var msg = "Compile failed:" + errorMsg;
                        Logging.Error("{0}", msg);

                        throw new Exception(msg);
                    }

                    Logging.Info("Assembly: {0}", res.PathToAssembly);
                    _compileCache.Store(sourceData, res.PathToAssembly);

                    //_compileCache.Unlock();
                    Tracing.Trace("]Compile");

                    return res.CompiledAssembly.CreateInstance(String.Format("Naiad.AutoGenerated.{0}", codeGen.CodeGenClass));
                }
            }
        }

        private static int TypeSize(Type t)
        {
            if (t == typeof(bool)) { return 1; }
            if (t == typeof(int)) { return sizeof(int); }
            if (t == typeof(long)) { return sizeof(long); }
            if (t == typeof(short)) { return sizeof(short); }
            if (t == typeof(double)) { return sizeof(double); }
            if (t == typeof(float)) { return sizeof(float); }
            if (t == typeof(Enum)) { return sizeof(int); }
            if (t == typeof(byte)) { return sizeof(byte); }
            if (t == typeof(uint)) { return sizeof(uint); }
            if (t == typeof(ulong)) { return sizeof(ulong); }
            if (IsNaiadable(t))
            {
                int length = 0;
                foreach (FieldInfo field in t.GetFields())
                {
                    Console.Error.WriteLine("Type {0} has field {1}", t, field.FieldType);

                    int fieldLength = TypeSize(field.FieldType);
                    if (fieldLength <= 0)
                        return -1;
                    length += fieldLength;
                }

                return length;
            }
            throw new NotImplementedException("TypeSize called on unknown type " + t);
            //return -1;
        }

        private static CodeExpression Compare(CodeExpression left, CodeBinaryOperatorType comp, CodeExpression right)
        {
            return new CodeBinaryOperatorExpression(left, comp, right);
        }

        private static CodeExpression Field(CodeExpression obj, String field)
        {
            return new CodeFieldReferenceExpression(obj, field);
        }

        private static CodeExpression Ref(CodeExpression var)
        {
            return new CodeDirectionExpression(FieldDirection.Ref, var);
        }

        private static CodeExpression Out(CodeExpression var)
        {
            return new CodeDirectionExpression(FieldDirection.Out, var);
        }

        private static CodeExpression Var(string name)
        {
            return new CodeVariableReferenceExpression(name);
        }

        private static CodeExpression Invoke(string targetClass, string method, params CodeExpression[] args)
        {
            return new CodeMethodInvokeExpression(new CodeTypeReferenceExpression(targetClass), method, args);
        }

        private static CodeExpression Invoke(CodeExpression obj, string method, params CodeExpression[] args)
        {
            return new CodeMethodInvokeExpression(new CodeMethodReferenceExpression(obj, method), args);
        }

        private static CodeCastExpression Cast(Type t, CodeExpression expr)
        {
            return new CodeCastExpression(t, expr);
        }

        private static CodeExpression Expr(string s)
        {
            return new CodeSnippetExpression(s);
        }

        private static CodeStatement Stmt(string s)
        {
            return new CodeSnippetStatement(s);
        }

        private static CodeStatement Return(CodeExpression e)
        {
            return new CodeMethodReturnStatement(e);
        }

        private static CodeStatement Return(string s)
        {
            return Return(Expr(s));
        }

        private static CodeExpression Default(Type t)
        {
            return IsNullable(t) ? Expr("null") : new CodeDefaultValueExpression(new CodeTypeReference(t));
        }

        private static CodeExpression Idx(CodeExpression array, CodeExpression idx)
        {
            return new CodeArrayIndexerExpression(array, idx);
        }

        private static CodeStatement Assign(CodeExpression left, CodeExpression right)
        {
            return new CodeAssignStatement(left, right);
        }

        private static CodeStatement Assign(string name, CodeExpression expr)
        {
            return new CodeAssignStatement(Var(name), expr);
        }

        private static CodeStatement ForLoop(
            CodeStatement init, CodeExpression test, CodeStatement incr, IEnumerable<CodeStatement> body)
        {
            return new CodeIterationStatement(init, test, incr, body.ToArray());
        }

        private static CodeBinaryOperatorExpression BinOp(CodeExpression left, CodeBinaryOperatorType op,
                                                          CodeExpression right)
        {
            return new CodeBinaryOperatorExpression(left, op, right);
        }

        private static CodeExpression Literal(object o)
        {
            return new CodePrimitiveExpression(o);
        }

        private static CodeStatement IfElse(CodeExpression test, CodeStatement[] trueStmt, CodeStatement[] falseStmt)
        {
            return new CodeConditionStatement(test, trueStmt, falseStmt);
        }

        private static CodeStatement CheckForSpace(
            CodeExpression subArrayExpr, int required, params CodeStatement[] stmts)
        {
            //NaiadLog.Assert(required > 0, "invalid size for type");
            return CheckForSpace(subArrayExpr, Literal(required), stmts);
        }

        private static CodeStatement CheckForSpace(
            CodeExpression subArrayExpr, CodeExpression required, params CodeStatement[] stmts)
        {
            return IfElse(
                Invoke(subArrayExpr, "EnsureAvailable", required), stmts, new[] { Stmt("return false;") });
        }

        private static CodeStatement Decl(string name, Type type, CodeExpression initializer = null)
        {
            return initializer != null
                       ? new CodeVariableDeclarationStatement(type, name, initializer)
                       : new CodeVariableDeclarationStatement(type, name);
        }

        public static bool IsNaiadable(Type t)
        {
            Type naiadableT = typeof(IEquatable<>).MakeGenericType(t);
            return naiadableT.IsAssignableFrom(t);
        }

        public static bool IsPrimitive(Type t)
        {
            if (t.IsArray)
                return false;
            return (t.IsPrimitive || t.Name.StartsWith("Dictionary"));
        }

        public static bool HasBuiltinSerializer(Type t)
        {
            return typeof(Serializers).GetMethod("Serialize", new Type[] { typeof(SubArray<byte>), t }) != null;
        }

        public static bool HasBuiltinDeserializer(Type t)
        {
            return typeof(Deserializers).GetMethod("TryDeserialize", new Type[] { typeof(RecvBuffer).MakeByRefType(), t.MakeByRefType() }) != null;
        }

        public static bool HasCustomSerialization(Type t)
        {
            return typeof(CustomNaiadableSerialization).IsAssignableFrom(t);
        }

        public static bool UsesVariableLengthSerialization(FieldInfo field)
        {
            return System.Attribute.GetCustomAttributes(field).Any(x => x is IntSerializationAttribute && ((IntSerializationAttribute)x).Mode == IntSerialization.VariableLength);
        }

        public static bool IsNullable(Type t)
        {
            return t.IsClass || t.IsInterface || t.IsArray;
        }

        public static List<FieldInfo> FieldMetadata(Type t)
        {
            var fields = new List<FieldInfo>();

            // ordering by name because apparently this isn't stable otherwise
            foreach (FieldInfo field in t.GetFields().OrderBy(x => x.Name))
            {
                if (!field.IsStatic)
                    fields.Add(field);
            }

            return fields;
        }


        private static CodeStatement LogIt(CodeExpression e)
        {
            return Stmt("");
        }

        private static CodeStatement[] Bail()
        {
            return new[]
                       {
                           //Stmt("System.Diagnostics.Debugger.Break();"),
                           Return("false")
                       };
        }

        #region Nested type: MethodHelper

        private class MethodHelper
        {
            private CodeMemberMethod _m;

            public MethodHelper(string name, Type returnType,
                                MemberAttributes attrs =
                                    MemberAttributes.Const | MemberAttributes.Final | MemberAttributes.Public)
            {
                _m = new CodeMemberMethod();
                _m.Name = name;
                _m.Attributes = attrs;
                _m.ReturnType = new CodeTypeReference(returnType);
            }

            public CodeMemberMethod Code
            {
                get { return _m; }
            }

            public CodeExpression Param(string name, Type type, FieldDirection dir = FieldDirection.In)
            {
                var param = new CodeParameterDeclarationExpression(new CodeTypeReference(type), name);
                param.Direction = dir;
                _m.Parameters.Add(param);
                return Var(name);
            }

            public void Add(CodeStatement stmt)
            {
                _m.Statements.Add(stmt);
            }

            public void Add(CodeExpression expr)
            {
                _m.Statements.Add(expr);
            }

            public void Return(CodeExpression expr)
            {
                Add(new CodeMethodReturnStatement(expr));
            }

            public void Return(string expr)
            {
                Return(new CodeSnippetExpression(expr));
            }

            public void Assign(string name, CodeExpression expr)
            {
                Add(new CodeAssignStatement(Var(name), expr));
            }

            public void AddAll(IEnumerable<CodeStatement> list)
            {
                foreach (CodeStatement expr in list)
                {
                    Add(expr);
                }
            }
        }

        #endregion

        #region Nested type: NaiadSerializationCodeGenerator

        /// <summary>
        /// Code generation logic for the serialization of a single type.
        /// </summary>
        public class NaiadSerializationCodeGenerator
        {
            public readonly string CodeGenClass;

            public readonly HashSet<Type> AllTypes;
            public readonly HashSet<Type> GeneratedTypes;
            public readonly Type Type;
            private readonly CodeTypeDeclaration classDecl;

            public NaiadSerializationCodeGenerator(Type t)
            {
                CodeGenClass = BadClassChars.Replace(FileNameForType(t), "_");
                classDecl = new CodeTypeDeclaration(CodeGenClass);
                GeneratedTypes = new HashSet<Type>();
                AllTypes = new HashSet<Type>();
                Type = t;
                AddType(t);

                while (AllTypes.Count > GeneratedTypes.Count)
                {
                    foreach (var lt in AllTypes.ToArray())
                    {
                        if (!GeneratedTypes.Contains(lt))
                        {
                            GenerateCodeForType(lt);
                        }
                    }
                }
            }

            public CodeCompileUnit CompileUnit
            {
                get
                {
                    var ret = new CodeCompileUnit();
                    var cn = new CodeNamespace("Naiad.AutoGenerated");
                    ret.Namespaces.Add(cn);
                    cn.Types.Add(classDecl);
                    return ret;
                }
            }
            public void AddType(Type t)
            {
                if (!AllTypes.Contains(t))
                {
                    AllTypes.Add(t);
                }

                foreach (Type gType in t.GetGenericArguments())
                {
                    AddType(gType);
                    //AddTypeReference(gType);
                }
            }

            public void GenerateCodeForType(Type t)
            {
                //Console.Error.WriteLine("Generating code for type {0}", t);

                if (t.IsAbstract)
                {
                    throw new InvalidOperationException(
                        String.Format("{0} is abstract, and cannot be deserialized.",
                        ClassNameForCompile(t)));
                }
                classDecl.Members.Add(GenerateNonStaticSerializeMethod(t));
                classDecl.Members.Add(GenerateNonStaticTryDeserializeMethod(t));
                classDecl.Members.Add(GenerateNonStaticGetHashCodeMethod(t));
                classDecl.Members.Add(GenerateNonStaticCompareMethod(t));
                classDecl.Members.Add(GenerateSerializeMethod(t));
                classDecl.Members.Add(GenerateTryDeserializeMethod(t));
                classDecl.Members.Add(GenerateGetHashCodeMethod(t));
                classDecl.Members.Add(GenerateCompareMethod(t));
                classDecl.BaseTypes.Add(
                    new CodeTypeReference(typeof(NaiadSerialization<>).MakeGenericType(t)));

                //Console.Error.WriteLine("Finished generating code for type {0}", t);


                GeneratedTypes.Add(t);
            }

            private CodeStatement[] CallBuiltinSerialize(CodeExpression dest, Type t, CodeExpression value)
            {
                List<CodeStatement> l = new List<CodeStatement>();

                l.Add(Decl("oldSubArray", typeof(SubArray<byte>), dest));
                l.Add(Assign(dest, Invoke(dest, "Serialize", value)));
                l.Add(IfElse(Invoke(Field(Var("oldSubArray"), "Count"), "Equals", Field(dest, "Count")),
                    new CodeStatement[] { Return("false") },
                    new CodeStatement[] { }));

                return l.ToArray();
            }

            private CodeStatement CallVarLengthSerialize(CodeExpression dest, CodeExpression value)
            {
                return IfElse(
                    Invoke("Naiad.Serializers", "SerializeVarLength", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
            }

            private CodeStatement CallSerialize(CodeExpression dest, CodeExpression value)
            {
#if DEBUG
                return IfElse(
                    Invoke("Naiad.CodeGeneration.AutoSerialization", "Serialize", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
#else
                return IfElse(
                    Invoke(CodeGenClass, "_Serialize", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
#endif
            }

            private CodeStatement CallVarLengthDeserialize(CodeExpression source, CodeExpression value)
            {
                return IfElse(
                    Invoke("Naiad.Deserializers", "TryDeserializeVarLength", Ref(source), Out(value)),
                    new CodeStatement[] { },
                    Bail());
            }

            private CodeStatement CallDeserialize(CodeExpression source, CodeExpression @var)
            {
#if DEBUG
                       return IfElse(
                        Invoke("Naiad.CodeGeneration.AutoSerialization", "TryDeserialize", Ref(source), Out(@var)),
                        new CodeStatement[] { },
                        Bail());
#else
                return IfElse(
                        Invoke(CodeGenClass, "_TryDeserialize", Ref(source), Out(@var)),
                        new CodeStatement[] { },
                        Bail());
#endif
            }



            public CodeMemberMethod GenerateNonStaticSerializeMethod(Type t)
            {
                var m = new MethodHelper("Serialize", typeof(bool), MemberAttributes.Public | MemberAttributes.Final);
                m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                m.Param("value", t, FieldDirection.In);

                m.Add(LogIt(Expr("destination.Count + \" - " + t.Name + "\"")));
                m.Add(Decl("originalDestination", typeof(SubArray<byte>), Var("destination")));
                m.Add(Decl("success", typeof(bool),
                           Invoke(CodeGenClass, "_Serialize", Ref(Var("destination")), Var("value"))));
                m.Add(IfElse(Var("success"),
                    new CodeStatement[] { },
                    new[] { Assign("destination", Var("originalDestination")) }));


                m.Add(LogIt(Expr("\" After: \" + destination.Count + \" - " + FileNameForType(t) + "\"")));
                m.Return(Var("success"));
                return m.Code;
            }

            public CodeMemberMethod GenerateNonStaticTryDeserializeMethod(Type t)
            {
                var m = new MethodHelper("TryDeserialize", typeof(bool),
                                         MemberAttributes.Final | MemberAttributes.Public);
                m.Param("source", typeof(RecvBuffer), FieldDirection.Ref);
                m.Param("value", t, FieldDirection.Out);
                m.Add(LogIt(Expr("source.CurrentPos +" + "\" : " + t.Name + "\"")));

                m.Add(LogIt(Expr("\" Before: \" + source.CurrentPos + \" - " + FileNameForType(t) + "\"")));
                m.Add(Decl("originalSource", typeof(RecvBuffer), Var("source")));

                m.Add(Decl("success", typeof(bool),
                           Invoke(CodeGenClass, "_TryDeserialize",
                                  Ref(new CodeVariableReferenceExpression("source")),
                                  Out(new CodeVariableReferenceExpression("value")))));

                m.Add(IfElse(
                          Var("success"),
                          new CodeStatement[] { },
                          new[] { Assign("source", Var("originalSource")) }));


                m.Add(LogIt(Expr("\" After: \" + source.CurrentPos + \" - " + FileNameForType(t) + "\"")));
                m.Return(Var("success"));
                return m.Code;
            }

            public CodeMemberMethod GenerateNonStaticGetHashCodeMethod(Type t)
            {
                var m = new MethodHelper("GetHashCode", typeof(int));
                m.Param("value", t);
                m.Return(new CodeMethodInvokeExpression(
                             new CodeTypeReferenceExpression(CodeGenClass), "_GetHashCode",
                             Var("value")));
                return m.Code;
            }

            public CodeMemberMethod GenerateNonStaticCompareMethod(Type t)
            {
                var m = new MethodHelper("Equals", typeof(bool));
                m.Param("a", t);
                m.Param("b", t);
                m.Return(Invoke(CodeGenClass, "_Equals", Var("a"), Var("b")));
                return m.Code;
            }

            public CodeMemberMethod GenerateCompareMethod(Type t)
            {
                var m = new MethodHelper("_Equals", typeof(bool),
                                         MemberAttributes.Static | MemberAttributes.Final);
                m.Param("a", t);
                m.Param("b", t);

                if (IsPrimitive(t) || t.IsEnum)
                {
                    m.Return(Invoke(Var("a"), "Equals", Var("b")));
                    return m.Code;
                }

                if (HasCustomSerialization(t))
                {
                    m.Add(Stmt("throw new System.Exception(\"Not Implemented\");"));
                    m.Return("false");
                    return m.Code;
                }

                var toCompare = new List<CodeExpression>();
                CodeExpression trueExpr = Literal(true);

                foreach (FieldInfo field in FieldMetadata(t))
                {
                    var aField = new CodeFieldReferenceExpression(Var("a"), field.Name);
                    var bField = new CodeFieldReferenceExpression(Var("b"), field.Name);

                    if (IsNaiadable(field.FieldType))
                    {
                        AddType(field.FieldType);
                        toCompare.Add(Invoke(CodeGenClass, "_Equals", aField, bField));
                    }
                    else
                    {
                        toCompare.Add(new CodeMethodInvokeExpression(aField, "Equals", bField));
                    }
                }

                m.Return(
                    toCompare.Aggregate(trueExpr,
                                        (a, b) =>
                                        new CodeBinaryOperatorExpression(a, CodeBinaryOperatorType.BooleanAnd, b)));

                return m.Code;
            }

            public CodeMemberMethod GenerateGetHashCodeMethod(Type t)
            {
                var m = new MethodHelper("_GetHashCode", typeof(int),
                                         MemberAttributes.Static | MemberAttributes.Final | MemberAttributes.Public);
                m.Param("value", t);
                m.Add(Decl("hash", typeof(int), Default(typeof(int))));

                if (IsPrimitive(t) || t.IsEnum)
                {
                    m.Assign("hash", Invoke(Var("value"), "GetHashCode"));
                    m.Return("hash");
                    return m.Code;
                }

                if (HasCustomSerialization(t))
                {
                    m.Add(Stmt("throw new System.Exception(\"Not Implemented\");"));
                    //m.Return(Var("hash"));
                    return m.Code;
                }

                foreach (FieldInfo field in FieldMetadata(t))
                {
                    CodeExpression fieldHash;
                    if (IsNaiadable(field.FieldType))
                    {
                        AddType(field.FieldType);
                        fieldHash = Invoke(CodeGenClass, "_GetHashCode",
                                           new CodeFieldReferenceExpression(Var("value"), field.Name));
                    }
                    else
                    {
                        var fieldRef = new CodeFieldReferenceExpression(Var("value"), field.Name);
                        fieldHash = Invoke("Naiad.HashCode", "SafeGetHashCode", fieldRef);
                    }

                    m.Assign("hash",
                             new CodeBinaryOperatorExpression(Var("hash"), CodeBinaryOperatorType.Add, fieldHash));
                }

                m.Return(Var("hash"));
                return m.Code;
            }

            public CodeMemberMethod GenerateSerializeMethod(Type t)
            {
                var m = new MethodHelper("_Serialize", typeof(bool), MemberAttributes.Static | MemberAttributes.Final | MemberAttributes.Public);

                CodeExpression dest = m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                CodeExpression value = m.Param("value", t, FieldDirection.In);
                m.AddAll(SerializeOrReturn(t, dest, value));

                m.Return(Expr("true"));
                return m.Code;
            }

            private IEnumerable<CodeStatement> SerializeOrReturn(Type t, CodeExpression dest, CodeExpression value)
            {
                if (IsNullable(t))
                {
                    AddType(typeof(bool));
                    var isNull = Compare(value, CodeBinaryOperatorType.IdentityEquality, Literal(null));
                    yield return CallSerialize(dest, isNull);

                    yield return IfElse(isNull, new[] { Return("true") }, new CodeStatement[] { });
                }

                if (HasCustomSerialization(t))
                {
                    yield return IfElse(Invoke(value, "Serialize", Ref(dest)),
                        new CodeStatement[] { },
                        new[] { Return("false") });
                }
                else if (t.IsEnum)
                {
                    // Cast to an int and serialize.
                    yield return CheckForSpace(
                        dest, 4, Assign(dest, Invoke(dest, "Serialize", Cast(typeof(int), value))));
                }
                    /*
                else if (t == typeof(String))
                {
                    CodeBinaryOperatorExpression needed =
                        BinOp(Field(value, "Length"), CodeBinaryOperatorType.Add, Literal(4));
                    yield return CheckForSpace(
                        dest, needed, Assign(dest, Invoke(dest, "Serialize", value)));
                }
                     */
                else if (IsPrimitive(t))
                {
                    yield return CheckForSpace(dest, TypeSize(t),
                                               Assign(dest, Invoke(dest, "Serialize", value)));
                }
                else if (HasBuiltinSerializer(t))
                {
                    yield return IfElse(new CodePrimitiveExpression(true),
                        CallBuiltinSerialize(dest, t, value), new CodeStatement[] {});
                }
                else if (t.IsArray)
                {
                    AddType(t.GetElementType());
                    yield return CallSerialize(dest, Field(value, "Length"));
                    yield return ForLoop(
                        Stmt("int i = 0"), BinOp(Expr("i"), CodeBinaryOperatorType.LessThan, Field(value, "Length")),
                        Stmt("++i"),
                        new CodeStatement[]
                            {
                                CallSerialize(dest, Idx(value, Var("i")))
                            });

                }
                else
                {
                    foreach (FieldInfo field in FieldMetadata(t))
                    {
                        if (Configuration.StaticConfiguration.VariableLengthSerialization && UsesVariableLengthSerialization(field))
                        {
                            yield return CallVarLengthSerialize(dest, Field(value, field.Name));
                        }
                        else
                        {
                            AddType(field.FieldType);
                            yield return CallSerialize(dest, Field(value, field.Name));
                        }
                    }
                }
            }

            private IEnumerable<CodeStatement> DeserializeOrReturn(Type t, CodeExpression source, CodeExpression value)
            {
                if (IsNullable(t))
                {
                    AddType(typeof(bool));
                    yield return Assign(value, Default(t));

                    CodeExpression isNullRef = Var("isNull");

                    yield return CallDeserialize(source, isNullRef);

                    yield return IfElse(
                        BinOp(isNullRef, CodeBinaryOperatorType.ValueEquality, Literal(true)),
                        new[] { Assign(value, Literal(null)), Return("true") },
                        new CodeStatement[] { });
                }

                if (HasCustomSerialization(t))
                {
                    yield return Assign(value, new CodeObjectCreateExpression(t));
                    yield return IfElse(
                        Invoke(value, "TryDeserialize", Ref(source)),
                        new CodeStatement[] { },
                        Bail());
                }
                else if (t.IsEnum)
                {
                    yield return Assign(value, Default(t));
                    yield return Decl("scratch", typeof(int), Default(typeof(int)));
                    yield return
                        IfElse(
                            Invoke("Deserializers", "TryDeserialize", Ref(source), Out(Var("scratch"))),
                            new[] { Assign(value, Cast(t, Var("scratch"))) },
                            Bail());
                }
                else if (IsPrimitive(t) || HasBuiltinDeserializer(t))
                {
                    yield return Assign(value, Default(t));
                    yield return
                        IfElse(
                            Invoke("Deserializers", "TryDeserialize", Ref(source), Out(value)),
                            new CodeStatement[] { },
                            Bail());
                }
                else if (t.IsArray)
                {
                    AddType(t.GetElementType());
                    AddType(typeof(int));

                    yield return new CodeVariableDeclarationStatement(typeof(int), "len", Literal(0));
                    yield return CallDeserialize(source, Var("len"));

                    var numDimensions = 0;// t.GetArrayRank();
                    var tt = t;
                    while (tt.IsArray)
                    {
                        numDimensions++;
                        tt = tt.GetElementType();
                    }


                    //var arrayDecl = new CodeCastExpression(new CodeTypeReference(t), 

                    //yield return Stmt("var q = ");
                    //yield return new CodeExpressionStatement(new CodeTypeReferenceExpression(t));

                    //var arrayDecl = new CodeArrayCreateExpression(tt.MakeArrayType(numDimensions), Var("len"));
                    // yield return Assign(value, arrayDecl);


                    var arrayDecl = "[len]";

                    for (var i = 0; i < numDimensions - 1; ++i)
                    {
                        arrayDecl += "[]";
                    }

                    arrayDecl += ";";

                    yield return Stmt("var q = new ");
                    yield return Stmt(ClassNameForCompile(tt));
                    yield return Stmt(arrayDecl);

                    yield return Assign(value, Expr("q"));

                    yield return ForLoop(
                        Stmt("int i = 0"), Expr("i < len"), Stmt("++i"),
                        new CodeStatement[]
                            {
                                CallDeserialize(source, Idx(value, Var("i")))
                            });
                }
                else
                {
                    yield return Assign(value, new CodeObjectCreateExpression(t));
                    foreach (FieldInfo field in FieldMetadata(t))
                    {
                        if (!field.IsPublic)
                            throw new Exception("Can't deserialize private field " + t.FullName + "." + field.Name);
                        if (field.IsInitOnly)
                            throw new Exception("Can't deserialize readonly field " + t.FullName + "." + field.Name);

                        if (Configuration.StaticConfiguration.VariableLengthSerialization && UsesVariableLengthSerialization(field))
                        {
                            yield return CallVarLengthDeserialize(source, Field(value, field.Name));
                        }
                        else
                        {
                            AddType(field.FieldType);
                            yield return CallDeserialize(source, Field(value, field.Name));
                        }
                    }
                }
            }
            public CodeMemberMethod GenerateTryDeserializeMethod(Type t)
            {
                var m = new MethodHelper("_TryDeserialize", typeof(bool),
                                         MemberAttributes.Public | MemberAttributes.Final | MemberAttributes.Static);

                m.Param("source", typeof(RecvBuffer), FieldDirection.Ref);
                m.Param("value", t, FieldDirection.Out);

                if (IsNullable(t)) 
                    m.Add(Decl("isNull", typeof(bool), Literal(false)));

                m.AddAll(DeserializeOrReturn(t, Var("source"), Var("value")));
                m.Return("true");
                return m.Code;
            }
        }

        #endregion
    }
}
