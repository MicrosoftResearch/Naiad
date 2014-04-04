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

using System;
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
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Research.Naiad.CodeGeneration
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

    public interface NaiadSerialization<T>// : IEqualityComparer<T>
    {
        bool Serialize(ref SubArray<byte> destination, T value);

        int TrySerializeMany(ref SubArray<byte> destination, ArraySegment<T> values);

        bool TryDeserialize(ref RecvBuffer source, out T value);
    }

    public interface SerializationCodeGenerator
    {
        NaiadSerialization<T> GetSerializer<T>();
        int MajorVersion { get; }
        int MinorVersion { get; }
    }

    internal abstract class BaseSerializationCodeGenerator : SerializationCodeGenerator
    {
        public abstract int MajorVersion { get; }
        public abstract int MinorVersion { get; }

        private readonly Dictionary<Type, object> serializerCache;

        public NaiadSerialization<T> GetSerializer<T>()
        {
            lock (this)
            {
                Type t = typeof(T);
                object serializer;

                if (!this.serializerCache.TryGetValue(t, out serializer))
                {
                    AutoSerialization.SerializationCodeGeneratorForType gen = this.GetCodeGeneratorForType(t);
                    serializer = gen.GenerateSerializer<T>();
                    this.serializerCache[t] = serializer;
                }

                return (NaiadSerialization<T>)serializer;
            }
        }

        protected BaseSerializationCodeGenerator()
        {
            this.serializerCache = new Dictionary<Type, object>();
        }

        protected abstract AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t);
    }

    internal static class Serialization
    {
        public static SerializationCodeGenerator GetCodeGeneratorForVersion(int majorVersion, int minorVersion)
        {
            if (minorVersion != 0)
                throw new InvalidOperationException(string.Format("No code generator available for version {0}.{1}", majorVersion, minorVersion));
            switch (majorVersion)
            {
                case 1:
                    return new LegacySerializationCodeGenerator();
                case 2:
                    return new InlineSerializationCodeGenerator();
                default:
                    throw new InvalidOperationException(string.Format("No code generator available for version {0}.{1}", majorVersion, minorVersion));
            }
        }
    }

    internal class LegacySerializationCodeGenerator : BaseSerializationCodeGenerator
    {
        public override int MajorVersion
        {
            get { return 1; }
        }

        public override int MinorVersion
        {
            get { return 0; }
        }

        protected override AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t)
        {
            return new AutoSerialization.NaiadSerializationCodeGenerator(t);
        }
    }

    internal class InlineSerializationCodeGenerator : BaseSerializationCodeGenerator
    {
        public override int MajorVersion
        {
            get { return 2; }
        }

        public override int MinorVersion
        {
            get { return 0; }
        }

        protected override AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t)
        {
            return new AutoSerialization.InlineNaiadSerializationCodeGenerator(t);
        }
    }

    /*
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
    */

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

    public static class AutoSerialization
    {
        private static readonly Regex BadClassChars = new Regex("[^A-Za-z0-9_]");

        private static object _codegenLock = new object();

        private static CompileCache _compileCache = new CompileCache(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
        private static Dictionary<Type, object> _codeGenCache = new Dictionary<Type, object>();

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

#if false
        public static NaiadSerialization<T> GetSerializer<T>()
        {
            lock (_codegenLock)
            {
                Type t = typeof(T);

                if (!_codeGenCache.ContainsKey(t))
                {
                    SerializationCodeGeneratorForType gen;
                    if (Configuration.StaticConfiguration.UseInlineSerialization)
                    {
                        gen = new InlineNaiadSerializationCodeGenerator(t);
                    }
                    else
                    {
                        gen = new NaiadSerializationCodeGenerator(t);
                    }
                    _codeGenCache[t] = gen.GenerateSerializer<T>();
                }

                return (NaiadSerialization<T>)_codeGenCache[t];
            }
        }
#endif

        public abstract class SerializationCodeGeneratorForType
        {
            protected readonly Type Type;
            protected readonly string GeneratedClassName;

            private readonly HashSet<string> referencedAssemblyLocations;

            public SerializationCodeGeneratorForType(Type type, string generatedClassName)
            {
                this.Type = type;
                this.GeneratedClassName = generatedClassName;
                this.referencedAssemblyLocations = new HashSet<string>();
            }

            public void AddReferencedAssembly(Assembly assembly)
            {
                this.referencedAssemblyLocations.Add(assembly.Location);
            }

            public abstract CodeCompileUnit GenerateCompileUnit();

            public NaiadSerialization<T> GenerateSerializer<T>()
            {

                Tracing.Trace("[Compile");
                using (var codeProvider = new CSharpCodeProvider())
                {
                    using (var sourceStream = new StringWriter())
                    {
                        var codeGenOpts = new CodeGeneratorOptions();
                        codeGenOpts.BlankLinesBetweenMembers = true;
                        codeGenOpts.ElseOnClosing = true;
                        codeGenOpts.BracingStyle = "Block";
                        codeGenOpts.IndentString = "  ";

                        codeProvider.GenerateCodeFromCompileUnit(this.GenerateCompileUnit(), sourceStream, codeGenOpts);

                        var sourceData = sourceStream.ToString();
                        
                        var sourcePath = FileNameForType(this.Type);
                        sourcePath = BadClassChars.Replace(sourcePath, "_");
                        sourcePath = Path.Combine(_compileCache.SourceDir, sourcePath + ".cs");
                        sourcePath = Path.GetFullPath(sourcePath);

                        // Delete any pre-existing source file.
                        File.Delete(sourcePath);

                        File.WriteAllText(sourcePath, sourceData);

                        Logging.Info("Compiling codegen for {0}", this.Type.FullName);

                        var compilerOpts = new CompilerParameters();
                        compilerOpts.GenerateInMemory = false;
#if DEBUG
                        compilerOpts.CompilerOptions = "/debug /unsafe";
#else
                        compilerOpts.CompilerOptions = "/optimize+ /unsafe";
                        compilerOpts.TreatWarningsAsErrors = false;
#endif
                        compilerOpts.IncludeDebugInformation = true;

                        compilerOpts.ReferencedAssemblies.Add("System.dll");
                        compilerOpts.ReferencedAssemblies.Add("System.Core.dll");

                        compilerOpts.ReferencedAssemblies.AddRange(this.referencedAssemblyLocations.ToArray());
                        compilerOpts.ReferencedAssemblies.Add(typeof(AutoSerialization).Assembly.Location);

                        compilerOpts.GenerateInMemory = false; //default

                        compilerOpts.TempFiles = new TempFileCollection(_compileCache.ObjectDir, true);
                        compilerOpts.IncludeDebugInformation = true;
                        compilerOpts.TempFiles.KeepFiles = true;

                        var res = codeProvider.CompileAssemblyFromSource(compilerOpts, new string[] { sourceData });

                        if (res.Errors.Count > 0)
                        {
                            var errorMsg = res.Errors.Cast<CompilerError>().Aggregate("", (current, ce) => current + (ce + "\n"));
                            var msg = "Compile failed:" + errorMsg;
                            Console.Error.WriteLine("Fatal: errors occurred during the generation of a serializer for type {0}:\n{1}", typeof(T).FullName, errorMsg);

                            throw new Exception(string.Format("Code generation for {0} failed", typeof(T).FullName));
                        }

                        Logging.Info("Assembly: {0}", res.PathToAssembly);
                        _compileCache.Store(sourceData, res.PathToAssembly);

                        //_compileCache.Unlock();
                        Tracing.Trace("]Compile");

                        return (NaiadSerialization<T>)res.CompiledAssembly.CreateInstance(String.Format("Microsoft.Research.Naiad.AutoGenerated.{0}", this.GeneratedClassName));
                    }
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

        private static CodeStatement If(CodeExpression test, CodeStatement trueStmt)
        {
            return new CodeConditionStatement(test, trueStmt);
        }


        private static CodeStatement If(CodeExpression test, CodeStatement[] trueStmts)
        {
            return new CodeConditionStatement(test, trueStmts);
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

        public class InlineNaiadSerializationCodeGenerator : SerializationCodeGeneratorForType
        {
            private readonly CodeTypeDeclaration classDecl;

            public InlineNaiadSerializationCodeGenerator(Type t) 
                : base(t, BadClassChars.Replace(FileNameForType(t), "_"))
            {
                this.classDecl = new CodeTypeDeclaration(GeneratedClassName);
                this.GenerateCode();
            }

            public override CodeCompileUnit GenerateCompileUnit()
            {
                var ret = new CodeCompileUnit();
                var cn = new CodeNamespace("Microsoft.Research.Naiad.AutoGenerated");
                ret.Namespaces.Add(cn);
                cn.Types.Add(this.classDecl);
                return ret;
            }

            public bool CanGenerateCode { get { return true; } }

            private void GenerateCode() 
            {
                if (!this.CanGenerateCode)
                    throw new InvalidOperationException(string.Format("Cannot generate serializer for type {0}", this.Type.FullName));

                classDecl.Members.Add(this.GenerateSerializeMethod());
                classDecl.Members.Add(this.GenerateTryDeserializeMethod());
                classDecl.Members.Add(this.GenerateTrySerializeManyMethod());
                classDecl.BaseTypes.Add(
                    new CodeTypeReference(typeof(NaiadSerialization<>).MakeGenericType(this.Type)));
            }

            private int tempVariableCount = 0;
            private string GenerateTempVariableName(string template)
            {
                return string.Format("__{0}_{1}", template, this.tempVariableCount++);
            }

            private IEnumerable<CodeStatement> GeneratePrimitiveSerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Debug.Assert(t.IsPrimitive && !t.IsArray);
                CodePrimitiveExpression size = new CodePrimitiveExpression(Marshal.SizeOf(t));

                yield return If(new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.LessThan, size), Return(failureReturnExpression));
                yield return Assign(Expr(string.Format("*({0}*)currentPosition", t.FullName)), toSerialize);
                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, size));
                yield return Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, size));

            }

            private IEnumerable<CodeStatement> GeneratePrimitiveDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsPrimitive && !t.IsArray);
                CodePrimitiveExpression size = new CodePrimitiveExpression(Marshal.SizeOf(t));

                yield return Assign(toDeserialize, Expr(string.Format("*({0}*)currentPosition", t.FullName)));
                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, size));
                yield return Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, size));

            }

            private IEnumerable<CodeStatement> GenerateEnumSerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Debug.Assert(t.IsEnum);
                return this.GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, new CodeCastExpression(typeof(int), toSerialize), typeof(int), failureReturnExpression);
            }

            private IEnumerable<CodeStatement> GenerateEnumDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsEnum);
                CodePrimitiveExpression size = new CodePrimitiveExpression(sizeof(int));

                yield return Assign(toDeserialize, new CodeCastExpression(t, Expr("*(int*)currentPosition")));
                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, size));
                yield return Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, size));

            }

            private const int NULL_ARRAY_MARKER = -1;
             
            private IEnumerable<CodeStatement> GenerateArraySerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Debug.Assert(t.IsArray);



                List<CodeStatement> nullArrayStmts = new List<CodeStatement>();

                foreach (CodeStatement stmt in this.GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, new CodePrimitiveExpression(NULL_ARRAY_MARKER), typeof(int), failureReturnExpression))
                    nullArrayStmts.Add(stmt);

                List<CodeStatement> arrayStmts = new List<CodeStatement>();


                CodeFieldReferenceExpression arrayLength = new CodeFieldReferenceExpression(toSerialize, "Length");

                // Count n, followed by n elements.
                foreach (CodeStatement stmt in this.GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, arrayLength, typeof(int), failureReturnExpression))
                    arrayStmts.Add(stmt);

                string iterationVar = this.GenerateTempVariableName("iteration");

                CodeIterationStatement loop = new CodeIterationStatement(Decl(iterationVar, typeof(int), new CodePrimitiveExpression(0)), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.LessThan, arrayLength), Assign(Var(iterationVar), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.Add, new CodePrimitiveExpression(1))));
                foreach (CodeStatement stmt in this.GenerateSerializeInstructions(currentPosition, bytesRemaining, new CodeArrayIndexerExpression(toSerialize, Var(iterationVar)), t.GetElementType(), failureReturnExpression))
                    loop.Statements.Add(stmt);

                arrayStmts.Add(loop);



                yield return IfElse(new CodeBinaryOperatorExpression(toSerialize, CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(null)),
                    nullArrayStmts.ToArray(), arrayStmts.ToArray());



            }

            private IEnumerable<CodeStatement> GenerateArrayDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsArray);

                string lengthVar = this.GenerateTempVariableName("length");
                yield return Decl(lengthVar, typeof(int));

                // Count n, followed by n elements.
                foreach (CodeStatement stmt in this.GeneratePrimitiveDeserializeInstructions(currentPosition, bytesRemaining, Var(lengthVar), typeof(int)))
                    yield return stmt;

                List<CodeStatement> nullArrayStmts = new List<CodeStatement>();

                nullArrayStmts.Add(Assign(toDeserialize, new CodePrimitiveExpression(null)));

                List<CodeStatement> arrayStmts = new List<CodeStatement>();



                arrayStmts.Add(Assign(toDeserialize, new CodeArrayCreateExpression(t, Var(lengthVar))));

                CodeFieldReferenceExpression arrayLength = new CodeFieldReferenceExpression(toDeserialize, "Length");


                string iterationVar = this.GenerateTempVariableName("iteration");

                CodeIterationStatement loop = new CodeIterationStatement(Decl(iterationVar, typeof(int), new CodePrimitiveExpression(0)), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.LessThan, arrayLength), Assign(Var(iterationVar), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.Add, new CodePrimitiveExpression(1))));
                foreach (CodeStatement stmt in this.GenerateDeserializeInstructions(currentPosition, bytesRemaining, new CodeArrayIndexerExpression(toDeserialize, Var(iterationVar)), t.GetElementType()))
                    loop.Statements.Add(stmt);

                arrayStmts.Add(loop);


                yield return IfElse(new CodeBinaryOperatorExpression(Var(lengthVar), CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(NULL_ARRAY_MARKER)),
                    nullArrayStmts.ToArray(), arrayStmts.ToArray());




            }

            private IEnumerable<CodeStatement> GenerateLegacyStructSerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Debug.Assert(t.IsValueType && t.GetFields().All(x => x.IsPublic && !x.IsInitOnly));

                foreach (var field in t.GetFields().Where(f => !f.IsStatic).OrderBy(f => f.Name))
                {
                    foreach (CodeStatement stmt in this.GenerateSerializeInstructions(currentPosition, bytesRemaining, new CodeFieldReferenceExpression(toSerialize, field.Name), field.FieldType, failureReturnExpression))
                        yield return stmt;
                }

            }

            private IEnumerable<CodeStatement> GenerateLegacyStructDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsValueType && t.GetFields().Where(x => !x.IsStatic).All(x => x.IsPublic && !x.IsInitOnly));

                yield return Assign(toDeserialize, new CodeObjectCreateExpression(t));

                foreach (var field in t.GetFields().Where(f => !f.IsStatic).OrderBy(f => f.Name))
                {
                    foreach (CodeStatement stmt in this.GenerateDeserializeInstructions(currentPosition, bytesRemaining, new CodeFieldReferenceExpression(toDeserialize, field.Name), field.FieldType))
                        yield return stmt;
                }

            }

            /// <summary>
            /// Recursively enumerates the parameters of a generic type, and adds their assemblies
            /// as dependencies for the generated code.
            /// <param name="t"></param>
            private void AddAssembliesForGenericParameters(Type t)
            {
                this.AddReferencedAssembly(t.Assembly);
                if (t.IsGenericType)
                {
                    foreach (Type parameter in t.GetGenericArguments())
                        this.AddAssembliesForGenericParameters(parameter);
                }
            }

            private IEnumerable<CodeStatement> GenerateDotnetBinarySerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Debug.Assert(t.IsSerializable);

                this.AddAssembliesForGenericParameters(t);
                
                string tempBufferVar = this.GenerateTempVariableName("tempBuffer");
                string formatterVar = this.GenerateTempVariableName("formatter");
                string memoryStreamVar = this.GenerateTempVariableName("memoryStream");

                yield return Decl(tempBufferVar, typeof(byte[]));
                
                yield return new CodeSnippetStatement(string.Format("using (System.IO.MemoryStream {0} = new System.IO.MemoryStream()) {{", memoryStreamVar));

                yield return Decl(formatterVar, typeof(BinaryFormatter), new CodeObjectCreateExpression(typeof(BinaryFormatter)));
                
                yield return new CodeExpressionStatement(new CodeMethodInvokeExpression(Var(formatterVar), "Serialize", Var(memoryStreamVar), toSerialize));

                yield return Assign(Var(tempBufferVar), new CodeMethodInvokeExpression(Var(memoryStreamVar), "ToArray"));

                yield return new CodeSnippetStatement("}");

                foreach (CodeStatement stmt in this.GenerateArraySerializeInstructions(currentPosition, bytesRemaining, Var(tempBufferVar), typeof(byte[]), failureReturnExpression))
                    yield return stmt;

            }

            private IEnumerable<CodeStatement> GenerateDotnetBinaryDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsSerializable);

                string tempBufferVar = this.GenerateTempVariableName("tempBuffer");
                string formatterVar = this.GenerateTempVariableName("formatter");
                string memoryStreamVar = this.GenerateTempVariableName("memoryStream");

                yield return Decl(tempBufferVar, typeof(byte[]));

                foreach (CodeStatement stmt in this.GenerateArrayDeserializeInstructions(currentPosition, bytesRemaining, Var(tempBufferVar), typeof(byte[])))
                    yield return stmt;

                yield return new CodeSnippetStatement(string.Format("using (System.IO.MemoryStream {0} = new System.IO.MemoryStream({1})) {{", memoryStreamVar, tempBufferVar));

                yield return Decl(formatterVar, typeof(BinaryFormatter), new CodeObjectCreateExpression(typeof(BinaryFormatter)));

                yield return Assign(toDeserialize, new CodeCastExpression(t, new CodeMethodInvokeExpression(Var(formatterVar), "Deserialize", Var(memoryStreamVar))));

                yield return new CodeSnippetStatement("}");

            }

            private IEnumerable<CodeStatement> GenerateSerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                this.AddReferencedAssembly(t.Assembly);
                if (t.IsArray)
                    return GenerateArraySerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (t.IsEnum)
                    return GenerateEnumSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (t.IsPrimitive)
                    return GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (t.IsValueType && t.GetFields().All(x => x.IsPublic && !x.IsInitOnly))
                    return GenerateLegacyStructSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (t.IsSerializable)
                    return GenerateDotnetBinarySerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else
                {
                    Logging.Error("Cannot generate serializer for type: {0}", t.FullName);
                    throw new NotImplementedException(string.Format("Cannot generate serializer for type: {0}", t.FullName));
                }
            }

            private IEnumerable<CodeStatement> GenerateDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                this.AddReferencedAssembly(t.Assembly);
                if (t.IsArray)
                    return GenerateArrayDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (t.IsEnum)
                    return GenerateEnumDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (t.IsPrimitive)
                    return GeneratePrimitiveDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (t.IsValueType && t.GetFields().All(x => x.IsPublic && !x.IsInitOnly))
                    return GenerateLegacyStructDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (t.IsSerializable)
                    return GenerateDotnetBinaryDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else
                {
                    Logging.Error("Cannot generate serializer for type: {0}", t.FullName);
                    throw new NotImplementedException(string.Format("Cannot generate serializer for type: {0}", t.FullName));
                }
            }

            public CodeMemberMethod GenerateSerializeMethod()
            {
                var m = new MethodHelper("Serialize", typeof(bool), MemberAttributes.Final | MemberAttributes.Public);
                CodeExpression dest = m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                CodeExpression value = m.Param("value", this.Type, FieldDirection.In);

                m.Add(Decl("bytesRemaining", typeof(int), Expr("destination.Array.Length - destination.Count")));

                // Must test for emptiness before taking the fixed pointer, because it performs a bounds check.
                m.Add(If(new CodeBinaryOperatorExpression(Var("bytesRemaining"), CodeBinaryOperatorType.LessThanOrEqual, new CodePrimitiveExpression(0)), Return(new CodePrimitiveExpression(false))));

                m.Add(new CodeSnippetStatement("unsafe { fixed (byte* destinationPtr = &destination.Array[destination.Count]) {"));

                m.Add(Decl("currentPosition", typeof(byte*), Expr("destinationPtr")));
                
                m.AddAll(this.GenerateSerializeInstructions(new CodeVariableReferenceExpression("currentPosition"), new CodeVariableReferenceExpression("bytesRemaining"), value, this.Type, new CodePrimitiveExpression(false)));

                m.Add(new CodeSnippetStatement("} }"));


                m.Add(Assign(Expr("destination.Count"), Expr("destination.Array.Length - bytesRemaining")));

                m.Return(Expr("true"));
                return m.Code;
            }

            public CodeMemberMethod GenerateTrySerializeManyMethod()
            {
                var m = new MethodHelper("TrySerializeMany", typeof(int), MemberAttributes.Final | MemberAttributes.Public);
                CodeExpression dest = m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                CodeExpression values = m.Param("values", typeof(ArraySegment<>).MakeGenericType(this.Type));

                m.Add(Decl("bytesRemaining", typeof(int), Expr("destination.Array.Length - destination.Count")));

                // Must test for emptiness before taking the fixed pointer, because it performs a bounds check.
                m.Add(If(new CodeBinaryOperatorExpression(Var("bytesRemaining"), CodeBinaryOperatorType.LessThanOrEqual, new CodePrimitiveExpression(0)), Return(new CodePrimitiveExpression(0))));

                m.Add(Decl("numWritten", typeof(int), Literal(0)));

                m.Add(new CodeSnippetStatement("unsafe { fixed (byte* destinationPtr = &destination.Array[destination.Count]) {"));

                m.Add(Decl("currentPosition", typeof(byte*), Expr("destinationPtr")));
                
                var forLoop = new CodeIterationStatement(Stmt("int i = 0"), Expr("i < values.Count"), Stmt("++i, ++numWritten"));

                forLoop.Statements.Add(Decl("value", this.Type, Expr("values.Array[i + values.Offset]")));

                forLoop.Statements.AddRange(this.GenerateSerializeInstructions(new CodeVariableReferenceExpression("currentPosition"), new CodeVariableReferenceExpression("bytesRemaining"), Expr("value"), this.Type, Expr("numWritten")).ToArray());

                forLoop.Statements.Add(Stmt("destination.Count = destination.Array.Length - bytesRemaining;"));

                //forLoop.Statements.Add(Stmt("bool success = this.Serialize(ref destination, values.Array[i + values.Offset]);"));
                //forLoop.Statements.Add(Stmt("if (!success) break;"));

                m.Add(forLoop);

                m.Add(new CodeSnippetStatement("} }"));

                m.Return(Expr("numWritten"));

                return m.Code;
            }

            public CodeMemberMethod GenerateTryDeserializeMethod()
            {
                var m = new MethodHelper("TryDeserialize", typeof(bool),
                                         MemberAttributes.Public | MemberAttributes.Final);

                m.Param("source", typeof(RecvBuffer), FieldDirection.Ref);
                m.Param("value", this.Type, FieldDirection.Out);

                m.Add(Decl("bytesRemaining", typeof(int), Expr("source.End - source.CurrentPos")));

                m.Add(If(new CodeBinaryOperatorExpression(Var("bytesRemaining"), CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(0)), new CodeStatement[] { 
                Assign(Expr("value"), new CodeDefaultValueExpression(new CodeTypeReference(this.Type))), Return(new CodePrimitiveExpression(false)) }));

                m.Add(new CodeSnippetStatement("unsafe { fixed (byte* sourcePtr = &source.Buffer[source.CurrentPos]) {"));

                m.Add(Decl("currentPosition", typeof(byte*), Expr("sourcePtr")));

                m.AddAll(this.GenerateDeserializeInstructions(new CodeVariableReferenceExpression("currentPosition"), new CodeVariableReferenceExpression("bytesRemaining"), Expr("value"), this.Type));

                m.Add(new CodeSnippetStatement("} }"));

                m.Add(Assign(Expr("source.CurrentPos"), Expr("source.End - bytesRemaining")));

                m.Return("true");
                return m.Code;
            }


        }

        /// <summary>
        /// Code generation logic for the serialization of a single type.
        /// </summary>
        public class NaiadSerializationCodeGenerator : SerializationCodeGeneratorForType
        {
            
            public readonly HashSet<Type> AllTypes;
            public readonly HashSet<Type> GeneratedTypes;
            private readonly CodeTypeDeclaration classDecl;

            public NaiadSerializationCodeGenerator(Type t)
                : base(t, BadClassChars.Replace(FileNameForType(t), "_"))
            {
                classDecl = new CodeTypeDeclaration(this.GeneratedClassName);
                GeneratedTypes = new HashSet<Type>();
                AllTypes = new HashSet<Type>();
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

            public override CodeCompileUnit GenerateCompileUnit()
            {
                var ret = new CodeCompileUnit();
                var cn = new CodeNamespace("Microsoft.Research.Naiad.AutoGenerated");
                ret.Namespaces.Add(cn);
                cn.Types.Add(classDecl);
                return ret;
            }

            public void AddType(Type t)
            {
                this.AddReferencedAssembly(t.Assembly);

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
                if (t.IsAbstract)
                {
                    throw new InvalidOperationException(
                        String.Format("{0} is abstract, and cannot be deserialized.",
                        ClassNameForCompile(t)));
                }
                classDecl.Members.Add(GenerateNonStaticSerializeMethod(t));
                classDecl.Members.Add(GenerateNonStaticTryDeserializeMethod(t));
                classDecl.Members.Add(GenerateSerializeMethod(t));
                classDecl.Members.Add(GenerateTrySerializeManyMethod(t));
                classDecl.Members.Add(GenerateTryDeserializeMethod(t));
                classDecl.BaseTypes.Add(
                    new CodeTypeReference(typeof(NaiadSerialization<>).MakeGenericType(t)));

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
                    Invoke("Microsoft.Research.Naiad.Serializers", "SerializeVarLength", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
            }

            private CodeStatement CallSerialize(CodeExpression dest, CodeExpression value)
            {
                return IfElse(
                    Invoke(this.GeneratedClassName, "_Serialize", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
            }

            private CodeStatement CallVarLengthDeserialize(CodeExpression source, CodeExpression value)
            {
                return IfElse(
                    Invoke("Microsoft.Research.Naiad.Deserializers", "TryDeserializeVarLength", Ref(source), Out(value)),
                    new CodeStatement[] { },
                    Bail());
            }

            private CodeStatement CallDeserialize(CodeExpression source, CodeExpression @var)
            {
                return IfElse(
                        Invoke(this.GeneratedClassName , "_TryDeserialize", Ref(source), Out(@var)),
                        new CodeStatement[] { },
                        Bail());
            }



            public CodeMemberMethod GenerateNonStaticSerializeMethod(Type t)
            {
                var m = new MethodHelper("Serialize", typeof(bool), MemberAttributes.Public | MemberAttributes.Final);
                m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                m.Param("value", t, FieldDirection.In);

                m.Add(LogIt(Expr("destination.Count + \" - " + t.Name + "\"")));
                m.Add(Decl("originalDestination", typeof(SubArray<byte>), Var("destination")));
                m.Add(Decl("success", typeof(bool),
                           Invoke(this.GeneratedClassName, "_Serialize", Ref(Var("destination")), Var("value"))));
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
                           Invoke(this.GeneratedClassName, "_TryDeserialize",
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

            public CodeMemberMethod GenerateTrySerializeManyMethod(Type t)
            {
                var m = new MethodHelper("TrySerializeMany", typeof(int), MemberAttributes.Final | MemberAttributes.Public);
                CodeExpression dest = m.Param("destination", typeof(SubArray<byte>), FieldDirection.Ref);
                CodeExpression values = m.Param("values", typeof(ArraySegment<>).MakeGenericType(t));

                m.Add(Decl("bytesRemaining", typeof(int), Expr("destination.Array.Length - destination.Count")));

                m.Add(Decl("numWritten", typeof(int), Literal(0)));

                var forLoop = new CodeIterationStatement(Stmt("int i = 0"), Expr("i < values.Count"), Stmt("++i, ++numWritten"));

                forLoop.Statements.Add(Stmt("bool success = this.Serialize(ref destination, values.Array[i + values.Offset]);"));
                forLoop.Statements.Add(Stmt("if (!success) break;"));

                m.Add(forLoop);

                m.Return(Expr("numWritten"));

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
