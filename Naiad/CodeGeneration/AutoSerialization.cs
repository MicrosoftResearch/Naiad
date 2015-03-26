/*
 * Naiad ver. 0.6
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
//#define FIXED_STRING_SERIALIZATION
using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.CSharp;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Serialization
{

    /// <summary>
    /// Represents a serializer and deserializer for a particular element type.
    /// </summary>
    /// <typeparam name="TElement">The type of elements handled by this object.</typeparam>
    public interface NaiadSerialization<TElement>
    {
        /// <summary>
        /// Attempts to serialize the given element into the given destination buffer.
        /// </summary>
        /// <param name="destination">The destination buffer.</param>
        /// <param name="value">The element to serialize.</param>
        /// <returns>True if the serialization was successful, otherwise false.</returns>
        bool Serialize(ref SubArray<byte> destination, TElement value);

        /// <summary>
        /// Attempts to serialize the given contiguous array segment of elements into the given destination buffer.
        /// </summary>
        /// <param name="destination">The destination buffer.</param>
        /// <param name="values">The elements to serialize.</param>
        /// <returns>The count of elements successfully serialized.</returns>
        int TrySerializeMany(ref SubArray<byte> destination, ArraySegment<TElement> values);

        /// <summary>
        /// Attempts to deserialize an element from the given source buffer.
        /// </summary>
        /// <param name="source">The source buffer.</param>
        /// <param name="value">The deserialized element, if the method returns true.</param>
        /// <returns>True if deserialization was successful, otherwise false.</returns>
        bool TryDeserialize(ref RecvBuffer source, out TElement value);

        /// <summary>
        /// Attempts to deserialize several elements from the given source buffer into the given contiguous
        /// array segment.
        /// </summary>
        /// <param name="source">The source buffer.</param>
        /// <param name="target">An array segment to be populated with the deserialized elements.</param>
        /// <returns>The count of elements successfully deserialized.</returns>
        int TryDeserializeMany(ref RecvBuffer source, ArraySegment<TElement> target);
    }

    /// <summary>
    /// Represents a particular format for serialization and deserialization.
    /// </summary>
    public interface SerializationFormat
    {
        /// <summary>
        /// Returns a serializer and deserializer for a particular element type.
        /// </summary>
        /// <typeparam name="TElement">The type of elements to be serialized and/or deserialized.</typeparam>
        /// <returns>A serializer and deserializer.</returns>
        NaiadSerialization<TElement> GetSerializer<TElement>();

        /// <summary>
        /// Returns a serializer and deserializer for a particular element type, using a specific
        /// minor version of the serialization format.
        /// 
        /// N.B. The minor version must be less than or equal to the <see cref="MinorVersion"/> of
        ///      this object.
        /// </summary>
        /// <typeparam name="TElement">The type of elements to be serialized and/or deserialized.</typeparam>
        /// <param name="minorVersion">The minor version of the serialization format to use.</param>
        /// <returns>A serializer and deserializer.</returns>
        NaiadSerialization<TElement> GetSerializationForMinorVersion<TElement>(int minorVersion); 

        /// <summary>
        /// The major version of the serialization format. Different major versions may not be compatible with each other.
        /// </summary>
        int MajorVersion { get; }

        /// <summary>
        /// The minor version of the serialization format. An implementation of a particular major and minor version must support
        /// backwards compatibility with all serializers of the same major version and earlier minor versions.
        /// </summary>
        int MinorVersion { get; }

        /// <summary>
        /// Registers the methods of <typeparamref name="TSerializer"/> type as custom serialization and deserialization methods
        /// for elements of type <typeparamref name="TElement"/>.
        /// </summary>
        /// <remarks>This must be invoked before the first <see cref="Computation"/> is activated.</remarks>
        /// <typeparam name="TElement">The type of element to be serialized or deserialized.</typeparam>
        /// <typeparam name="TSerializer">The type of the custom serializer.</typeparam>
        void RegisterCustomSerialization<TElement, TSerializer>()
            where TSerializer : CustomSerialization<TElement>, new();
    }

    /// <summary>
    /// Encapsulates the custom code for serializing and deserializing objects of
    /// type <typeparamref name="TElement"/>.
    /// </summary>
    /// <remarks>Implementations of this interface should have a no-argument constructor,
    /// because the serialization code generator will attempt to instantiate serializers using the no-argument
    /// constructor. A type constraint on <see cref="SerializationFormat.RegisterCustomSerialization{TElement,TSerializer}"/>
    /// ensures that this is the case for all registered serializers.</remarks>
    /// <example>
    /// public unsafe class IntSerializer : CustomSerialization&lt;int&gt;
    /// {
    ///     public unsafe int TrySerialize(int value, byte* buffer, int limit)
    ///     {
    ///         if (limit &lt; 4)
    ///             return -1;
    ///         *(int*)buffer = value;
    ///         return 4;
    ///     }
    ///     
    ///     public unsafe int Deserialize(out int value, byte* buffer, int limit)
    ///     {
    ///         value = *(int*)buffer;
    ///         return 4;
    ///     }
    /// }
    /// 
    /// // Generated serialization code:
    /// byte* currentPosition = ...;
    /// int bytesRemaining = ...;
    /// int toSerialize = ...;
    /// 
    /// IntSerializer serializer = new IntSerializer();
    /// int bytesWritten = serializer.TrySerialize(value, currentPosition, bytesRemaining);
    /// if (bytesWritten &lt;= 0)
    ///     return false;
    /// else
    ///     currentPosition += bytesWritten;
    /// 
    /// // Generated deserialization code:
    /// byte* currentPosition = ...;
    /// int bytesRemaining = ...;
    /// int toDeserialize;
    /// 
    /// IntSerializer deserializer = new IntSerializer();
    /// int bytesRead = deserializer.Deserialize(out toDeserialize, currentPosition, bytesRemaining);
    /// currentPosition += bytesRead;
    /// bytesRemaining -= bytesRead;
    /// </example>
    /// <typeparam name="TElement">The type of element to be serialized or deserialized.</typeparam>
    /// <seealso cref="Microsoft.Research.Naiad.Serialization.SerializationFormat.RegisterCustomSerialization{TElement,TSerializer}"/>
    public interface CustomSerialization<TElement>
    {
        /// <summary>
        /// Attempts to serialize the given <paramref name="value"/> into the given
        /// <paramref name="buffer"/> with <paramref name="limit"/> bytes remaining.
        /// </summary>
        /// <param name="value">The value to be serialized.</param>
        /// <param name="buffer">The target buffer.</param>
        /// <param name="limit">The number of bytes remaining in <paramref name="buffer"/>.</param>
        /// <returns>If serialization was successful, returns the number of bytes written.
        /// If serialization was unsuccessful, returns a value less than or equal to 0.</returns>
        unsafe int TrySerialize(TElement value, byte* buffer, int limit);

        /// <summary>
        /// Deserializes into <paramref name="value"/> the next element from the given
        /// <paramref name="buffer"/>.
        /// </summary>
        /// <param name="value">Will be set to the deserialized value.</param>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="limit">The number of bytes remaining in <paramref name="buffer"/>.</param>
        /// <returns>The number of bytes read.</returns>
        unsafe int Deserialize(out TElement value, byte* buffer, int limit);
    }

    internal class CustomSerializationWrapper<TElement, TCustomSerialization> : NaiadSerialization<TElement>
        where TCustomSerialization : CustomSerialization<TElement>
    {
        private readonly TCustomSerialization innerSerializer;

        public CustomSerializationWrapper(TCustomSerialization innerSerializer)
        {
            this.innerSerializer = innerSerializer;
        }

        public unsafe bool Serialize(ref SubArray<byte> destination, TElement value)
        {
            int bytesRemaining = destination.Array.Length - destination.Count;
            if (bytesRemaining <= 0)
            {
                return false;
            }
            fixed (byte* destinationPtr = &destination.Array[destination.Count])
            {
                int bytesWritten = this.innerSerializer.TrySerialize(value, destinationPtr, bytesRemaining);
                if (bytesWritten <= 0)
                {
                    return false;
                }

                destination.Count += bytesWritten;
            }
            return true;
        }

        public unsafe int TrySerializeMany(ref SubArray<byte> destination, ArraySegment<TElement> values)
        {
            int bytesRemaining = destination.Array.Length - destination.Count;
            if (bytesRemaining <= 0)
            {
                return 0;
            }
            fixed (byte* destinationPtr = &destination.Array[destination.Count])
            {
                int totalBytesWritten = 0;
                for (int i = 0; i < values.Count; ++i)
                {
                    int bytesWritten = this.innerSerializer.TrySerialize(values.Array[values.Offset + i], destinationPtr + totalBytesWritten, bytesRemaining - totalBytesWritten);
                    if (bytesWritten <= 0)
                    {
                        destination.Count += totalBytesWritten;
                        return i;
                    }
                    totalBytesWritten += bytesWritten;
                }
                destination.Count += totalBytesWritten;
                return values.Count;
            }
        }

        public unsafe bool TryDeserialize(ref RecvBuffer source, out TElement value)
        {
            int bytesRemaining = source.End - source.CurrentPos;
            if (bytesRemaining <= 0)
            {
                value = default(TElement);
                return false;
            }
            fixed (byte* sourcePtr = &source.Buffer[source.CurrentPos])
            {
                int bytesRead = this.innerSerializer.Deserialize(out value, sourcePtr, bytesRemaining);
                source.CurrentPos += bytesRead;
                return true;
            }
        }

        public unsafe int TryDeserializeMany(ref RecvBuffer source, ArraySegment<TElement> target)
        {
            int bytesRemaining = source.End - source.CurrentPos;
            if (bytesRemaining <= 0)
            {
                return 0;
            }
            fixed (byte* sourcePtr = &source.Buffer[source.CurrentPos])
            {
                int totalBytesRead = 0;
                for (int i = 0; i < target.Count; ++i)
                {
                    int bytesRead = this.innerSerializer.Deserialize(out target.Array[target.Offset + i], sourcePtr, bytesRemaining - totalBytesRead);
                    if (bytesRead <= 0)
                    {
                        source.CurrentPos += totalBytesRead;
                        return i;
                    }
                    totalBytesRead += bytesRead;
                }
                source.CurrentPos += totalBytesRead;
                return target.Count;
            }
        }
    }

    /// <summary>
    /// Static factory class for making custom serializers
    /// </summary>
    public static class CustomSerialization
    {
        /// <summary>
        /// Make a Naiad serializer for a given type and custom serializer type with an argument-less constructor
        /// </summary>
        /// <typeparam name="TElement">Record type to serialize</typeparam>
        /// <typeparam name="TCustomSerialization">Custom serializer type</typeparam>
        /// <returns>Naiad serializer</returns>
        public static NaiadSerialization<TElement> MakeSerializer<TElement, TCustomSerialization>() where TCustomSerialization : CustomSerialization<TElement>, new()
        {
            return new CustomSerializationWrapper<TElement, TCustomSerialization>(new TCustomSerialization());
        }

        /// <summary>
        /// Make a Naiad serializer for a given type and custom serializer
        /// </summary>
        /// <typeparam name="TElement">Record type to serialize</typeparam>
        /// <typeparam name="TCustomSerialization">Custom serializer type</typeparam>
        /// <param name="instance">Custom serializer</param>
        /// <returns>Naiad serializer</returns>
        public static NaiadSerialization<TElement> MakeSerializer<TElement, TCustomSerialization>(TCustomSerialization instance) where TCustomSerialization : CustomSerialization<TElement>
        {
            return new CustomSerializationWrapper<TElement, TCustomSerialization>(instance);
        }
    }

    internal abstract class BaseSerializationCodeGenerator : SerializationFormat
    {
        public abstract int MajorVersion { get; }
        public abstract int MinorVersion { get; }

        private readonly Dictionary<Pair<Type, int>, object> serializerCache;

        private readonly Dictionary<Type, Type> customSerializationMapping;

        public NaiadSerialization<T> GetSerializationForMinorVersion<T>(int minorVersion)
        {
            if (minorVersion > this.MinorVersion)
                throw new ArgumentOutOfRangeException("minorVersion", minorVersion, string.Format("Cannot generate serializer for version {0}.{1} (highest supported is {0}.{2})", this.MajorVersion, minorVersion, this.MinorVersion));
            
            lock (this)
            {
                Type t = typeof(T);
                object serializer;

                if (!this.serializerCache.TryGetValue(t.PairWith(minorVersion), out serializer))
                {
                    AutoSerialization.SerializationCodeGeneratorForType gen = this.GetCodeGeneratorForType(t, minorVersion);
                    serializer = gen.GenerateSerializer<T>();
                    this.serializerCache[t.PairWith(minorVersion)] = serializer;
                }

                return (NaiadSerialization<T>)serializer;
            }

        }

        public NaiadSerialization<T> GetSerializer<T>()
        {
            return this.GetSerializationForMinorVersion<T>(this.MinorVersion);
        }

        internal bool HasCustomSerialization(Type t)
        {
            return this.customSerializationMapping.ContainsKey(t);
        }

        internal Type GetCustomSerializationType(Type t)
        {
            return this.customSerializationMapping[t];
        }

        public void RegisterCustomSerialization<TElement, TSerializer>()
            where TSerializer : CustomSerialization<TElement>, new()
        {
            this.customSerializationMapping[typeof(TElement)] = typeof(TSerializer);
        }

        protected BaseSerializationCodeGenerator()
        {
            this.serializerCache = new Dictionary<Pair<Type, int>, object>();
            this.customSerializationMapping = new Dictionary<Type, Type>();
        }

        protected abstract AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t, int minorVersion);
    }

    internal static class SerializationFactory
    {
        public static SerializationFormat GetCodeGeneratorForVersion(int majorVersion, int minorVersion)
        {
            //if (minorVersion != 0)
            //    throw new InvalidOperationException(string.Format("No code generator available for version {0}.{1}", majorVersion, minorVersion));
            switch (majorVersion)
            {
                case 1:
                    return new LegacySerializationCodeGenerator();
                case 2:
                    return new InlineSerializationCodeGenerator(minorVersion);
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

        protected override AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t, int minorVersion)
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

        private readonly int minorVersion;
        public override int MinorVersion { get { return this.minorVersion; }}
   
        protected override AutoSerialization.SerializationCodeGeneratorForType GetCodeGeneratorForType(Type t, int minorVersion)
        {
            return new AutoSerialization.InlineNaiadSerializationCodeGenerator(this, t, minorVersion);
        }

        public InlineSerializationCodeGenerator(int minorVersion)
        {
            if (minorVersion > 2)
                throw new ArgumentOutOfRangeException("minorVersion", minorVersion, string.Format("Minor version {0} not supported", minorVersion));
            this.minorVersion = minorVersion;
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

    /// <summary>
    /// Factory class for serialization code generators.
    /// </summary>
    public static class AutoSerialization
    {
        private static readonly Regex BadClassChars = new Regex("[^A-Za-z0-9_]");

        private static object _codegenLock = new object();

        private static CompileCache _compileCache = new CompileCache(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
        private static Dictionary<Type, object> _codeGenCache = new Dictionary<Type, object>();

        private static string FileNameForType(Type t)
        {
            var className = t.Name;
            foreach (var gt in t.GetGenericArguments())
                className = className + "-" + gt.Name;
            return className;
        }

        private static string ClassNameForCompile(Type t)
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

        internal abstract class SerializationCodeGeneratorForType
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

            private static void AddAssemblyToCollection(Dictionary<string,string> assemblies, string assemblyLocation)
            {
                string fileName = Path.GetFileName(assemblyLocation);
                if (!assemblies.ContainsKey(fileName))
                {
                    assemblies.Add(fileName, assemblyLocation);
                }
            }

            public NaiadSerialization<T> GenerateSerializer<T>()
            {

                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.Compile);

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
#if DEBUG
                        compilerOpts.CompilerOptions = "/debug /unsafe"; 
#else
                        compilerOpts.CompilerOptions = "/optimize+ /unsafe";
                        compilerOpts.TreatWarningsAsErrors = false;
#endif
                        compilerOpts.IncludeDebugInformation = true;

                        Dictionary<string, string> assemblies = new Dictionary<string, string>();

                        foreach (string location in this.referencedAssemblyLocations)
                        {
                            AddAssemblyToCollection(assemblies, location);
                        }
                        AddAssemblyToCollection(assemblies, typeof(AutoSerialization).Assembly.Location);
                        AddAssemblyToCollection(assemblies, "System.dll");
                        AddAssemblyToCollection(assemblies, "System.Core.dll");

                        foreach (string location in assemblies.Values)
                        {
                            compilerOpts.ReferencedAssemblies.Add(location);
                        }

                        compilerOpts.GenerateInMemory = false; //default

                        compilerOpts.TempFiles = new TempFileCollection(_compileCache.ObjectDir, true);
                        compilerOpts.IncludeDebugInformation = true;
                        compilerOpts.TempFiles.KeepFiles = true;

                        //Console.WriteLine("Serialization classes for {0} written to {1}", typeof(T), compilerOpts.TempFiles.BasePath);

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
                        NaiadTracing.Trace.RegionStop(NaiadTracingRegion.Compile);

                        return (NaiadSerialization<T>)res.CompiledAssembly.CreateInstance(String.Format("Microsoft.Research.Naiad.Serialization.AutoGenerated.{0}", this.GeneratedClassName));
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
            if (t == typeof(char)) { return sizeof(char); }
            if (t == typeof(double)) { return sizeof(double); }
            if (t == typeof(float)) { return sizeof(float); }
            if (t == typeof(Enum)) { return sizeof(int); }
            if (t == typeof(byte)) { return sizeof(byte); }
            if (t == typeof(uint)) { return sizeof(uint); }
            if (t == typeof(ulong)) { return sizeof(ulong); }
            //if (IsNaiadable(t))
            //{
            //    int length = 0;
            //    foreach (FieldInfo field in t.GetFields())
            //    {
            //        //Console.Error.WriteLine("Type {0} has field {1}", t, field.FieldType);

            //        int fieldLength = TypeSize(field.FieldType);
            //        if (fieldLength <= 0)
            //            return -1;
            //        length += fieldLength;
            //    }
                
            //    return length;
            //}
            throw new NotImplementedException("Serialization not supported for type " + t);
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

        private static bool IsPrimitive(Type t)
        {
            if (t.IsArray)
                return false;
            return (t.IsPrimitive || t.Name.StartsWith("Dictionary"));
        }

        private static bool HasBuiltinSerializer(Type t)
        {
            return typeof(Serializers).GetMethod("Serialize", new Type[] { typeof(SubArray<byte>), t }) != null;
        }

        private static bool HasBuiltinDeserializer(Type t)
        {
            return typeof(Deserializers).GetMethod("TryDeserialize", new Type[] { typeof(RecvBuffer).MakeByRefType(), t.MakeByRefType() }) != null;
        }

        private static bool HasCustomSerialization(Type t)
        {
            return false;
        }
        
        private static bool IsNullable(Type t)
        {
            return t.IsClass || t.IsInterface || t.IsArray;
        }

        private static List<FieldInfo> FieldMetadata(Type t)
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
            //return new CodeExpressionStatement(new CodeMethodInvokeExpression(new CodeMethodReferenceExpression(new CodeTypeReferenceExpression(typeof(Console)), "WriteLine"), e)); //Stmt("");
            return Stmt("");
        }

        private static CodeStatement WriteLine(CodeExpression formatString, params CodeExpression[] args)
        {

            return new CodeExpressionStatement(new CodeMethodInvokeExpression(new CodeTypeReferenceExpression(typeof (Console)), "WriteLine",
                new CodeExpression[] {formatString}.Concat(args).ToArray()));
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

        internal class InlineNaiadSerializationCodeGenerator : SerializationCodeGeneratorForType
        {
            private readonly CodeTypeDeclaration classDecl;

            private readonly BaseSerializationCodeGenerator format;

            private readonly int minorVersion;

            public InlineNaiadSerializationCodeGenerator(BaseSerializationCodeGenerator format, Type t, int minorVersion) 
                : base(t, BadClassChars.Replace(FileNameForType(t), "_"))
            {
                this.format = format;
                this.minorVersion = minorVersion;
                this.classDecl = new CodeTypeDeclaration(GeneratedClassName);
                this.GenerateCode();
            }

            public override CodeCompileUnit GenerateCompileUnit()
            {
                var ret = new CodeCompileUnit();
                var cn = new CodeNamespace("Microsoft.Research.Naiad.Serialization.AutoGenerated");
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
                classDecl.Members.Add(this.GenerateTryDeserializeManyMethod());
                classDecl.BaseTypes.Add(
                    new CodeTypeReference(typeof(NaiadSerialization<>).MakeGenericType(this.Type)));
            }

            private int tempVariableCount = 0;
            private string GenerateTempVariableName(string template)
            {
                return string.Format("__{0}_{1}", template, this.tempVariableCount++);
            }

            private CodeStatement GenerateSpaceCheck(CodeExpression bytesRemaining, CodeExpression targetSize,
                CodeExpression failureReturnExpression)
            {
                return If(new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.LessThan, targetSize), Return(failureReturnExpression));
            }

            public CodeMemberMethod GenerateTryDeserializeManyMethod()
            {
                var m = new MethodHelper("TryDeserializeMany", typeof(int),
                    MemberAttributes.Final | MemberAttributes.Public);
                m.Param("source", typeof(RecvBuffer), FieldDirection.Ref);
                m.Param("target", typeof(ArraySegment<>).MakeGenericType(this.Type));

                m.Add(Decl("bytesRemaining", typeof(int), Expr("source.End - source.CurrentPos")));

                m.Add(If(new CodeBinaryOperatorExpression(Var("bytesRemaining"), CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(0)), Return(new CodePrimitiveExpression(0))));

                m.Add(new CodeSnippetStatement("unsafe { fixed (byte* sourcePtr = &source.Buffer[source.CurrentPos]) {"));

                m.Add(Decl("currentPosition", typeof(byte*), Expr("sourcePtr")));

                var forLoop = new CodeIterationStatement(Stmt("int i = 0"), Expr("i < target.Count"), Stmt("++i"));

                forLoop.Statements.Add(
                    If(
                        new CodeBinaryOperatorExpression(Var("bytesRemaining"), CodeBinaryOperatorType.ValueEquality,
                            new CodePrimitiveExpression(0)), new CodeStatement[] { Assign(Expr("source.CurrentPos"), Expr("source.End")), Return("i") }));

                forLoop.Statements.AddRange(this.GenerateDeserializeInstructions(new CodeVariableReferenceExpression("currentPosition"), new CodeVariableReferenceExpression("bytesRemaining"), Expr("target.Array[i + target.Offset]"), this.Type).ToArray());

                m.Add(Assign(Expr("source.CurrentPos"), Expr("source.End - bytesRemaining")));

                //forLoop.Statements.Add(Stmt("bool success = this.Serialize(ref destination, values.Array[i + values.Offset]);"));
                //forLoop.Statements.Add(Stmt("if (!success) break;"));

                m.Add(forLoop);

                m.Add(new CodeSnippetStatement("} }"));

                m.Add(Assign(Expr("source.CurrentPos"), Expr("source.End - bytesRemaining")));

                m.Return("target.Count");
                return m.Code;


#if false
                return null;


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

#endif
            }

            private IEnumerable<CodeStatement> GenerateStringSerializeInstructions(
                CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining,
                CodeExpression toSerialize, CodeExpression failureReturnExpression)
            {
                CodeExpression length = new CodeFieldReferenceExpression(toSerialize, "Length");
                CodeExpression lengthBytes = BinOp(BinOp(length, CodeBinaryOperatorType.Multiply, Expr("sizeof(char)")) , CodeBinaryOperatorType.Add, Expr("sizeof(int)"));

                List<CodeStatement> nullStringStmts = new List<CodeStatement>();

                foreach (CodeStatement stmt in this.GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, new CodePrimitiveExpression(NULL_ARRAY_MARKER), typeof(int), failureReturnExpression))
                    nullStringStmts.Add(stmt);

                List<CodeStatement> stringStmts = new List<CodeStatement>();

                CodeExpression totalLengthInBytes = BinOp(Expr("sizeof(int)"), CodeBinaryOperatorType.Add, BinOp(length, CodeBinaryOperatorType.Multiply, Expr("sizeof(char)")));

                stringStmts.Add(GenerateSpaceCheck(bytesRemaining, totalLengthInBytes, failureReturnExpression));

                foreach (CodeStatement stmt in this.GeneratePrimitiveSerializeInstructionsWithoutCheck(currentPosition, length, typeof(int)))
                    stringStmts.Add(stmt);

#if FIXED_STRING_SERIALIZATION
                // Insert a fixed block to manipulate the string pointer directly.
                string stringPtrVar = this.GenerateTempVariableName("strPtr");
                string stringAliasVar = this.GenerateTempVariableName("strAlias");
                string sourceCharPtr = this.GenerateTempVariableName("sourceCharPtr");

                stringStmts.Add(Decl(stringAliasVar, typeof(string), toSerialize));

                stringStmts.Add(Stmt(string.Format("fixed (char* {0} = {1})", stringPtrVar, stringAliasVar) + 
                    "{"));

                stringStmts.Add(Decl(sourceCharPtr, typeof(char*), Var(stringPtrVar)));
#endif

                string iterationVar = this.GenerateTempVariableName("iteration");

                CodeIterationStatement loop = new CodeIterationStatement(Decl(iterationVar, typeof(int), new CodePrimitiveExpression(0)), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.LessThan, length), Assign(Var(iterationVar), new CodeBinaryOperatorExpression(Var(iterationVar), CodeBinaryOperatorType.Add, new CodePrimitiveExpression(1))));

#if FIXED_STRING_SERIALIZATION
                loop.Statements.Add(Stmt(string.Format("*(char*)currentPosition = *{0}++;", sourceCharPtr)));
                loop.Statements.Add(Stmt("currentPosition += sizeof(char);"));
                stringStmts.Add(loop);
#else
                loop.Statements.Add(
                    new CodeAssignStatement(Expr(string.Format("*(((char*)currentPosition) + {0})", iterationVar)),
                        new CodeArrayIndexerExpression(toSerialize, Var(iterationVar))));
                stringStmts.Add(loop);
                stringStmts.Add(Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, BinOp(length, CodeBinaryOperatorType.Multiply, Expr("sizeof(char)")))));
#endif
                
                stringStmts.Add(Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, totalLengthInBytes)));

#if FIXED_STRING_SERIALIZATION
                stringStmts.Add(Stmt("}"));
#endif
                    
                yield return
                    IfElse(BinOp(toSerialize, CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(null)),
                        nullStringStmts.ToArray(), stringStmts.ToArray());
            }

            private IEnumerable<CodeStatement> GenerateStringDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize)
            {
                string lengthVar = this.GenerateTempVariableName("length");
                yield return Decl(lengthVar, typeof(int));

                // Count n, followed by n elements.
                foreach (CodeStatement stmt in this.GeneratePrimitiveDeserializeInstructions(currentPosition, bytesRemaining, Var(lengthVar), typeof(int)))
                    yield return stmt;
                
                List<CodeStatement> nullStringStmts = new List<CodeStatement>();

                nullStringStmts.Add(Assign(toDeserialize, new CodePrimitiveExpression(null)));

                List<CodeStatement> stringStmts = new List<CodeStatement>();

                stringStmts.Add(Assign(toDeserialize,
                    new CodeObjectCreateExpression(typeof(string), new CodeCastExpression(typeof(char*), currentPosition), Expr("0"), Var(lengthVar))));

                stringStmts.Add(Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, BinOp(Var(lengthVar), CodeBinaryOperatorType.Multiply, Expr("sizeof(char)")))));
                stringStmts.Add(Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, BinOp(Var(lengthVar), CodeBinaryOperatorType.Multiply, Expr("sizeof(char)")))));

                yield return IfElse(new CodeBinaryOperatorExpression(Var(lengthVar), CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(NULL_ARRAY_MARKER)),
                    nullStringStmts.ToArray(), stringStmts.ToArray());
            }

            private IEnumerable<CodeStatement> GeneratePrimitiveSerializeInstructionsWithoutCheck(CodeVariableReferenceExpression currentPosition, CodeExpression toSerialize, Type t)
            {
                Debug.Assert(t.IsPrimitive && !t.IsArray);
                CodeExpression size = Expr(string.Format("sizeof({0})", t.FullName));

                yield return Assign(Expr(string.Format("*({0}*)currentPosition", t.FullName)), toSerialize);
                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, size));
                
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

            private IEnumerable<CodeStatement> GenerateCustomSerializeInstructions(
                CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining,
                CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                string serializerName = this.GenerateTempVariableName("customSerializer");
                Type customSerializerType = this.format.GetCustomSerializationType(t);
                this.AddReferencedAssembly(customSerializerType.Assembly);

                yield return
                    Decl(serializerName, customSerializerType, new CodeObjectCreateExpression(customSerializerType));

                string byteCounterName = this.GenerateTempVariableName("bytesWritten");
                yield return
                    Decl(byteCounterName, typeof (int),
                        Invoke(Expr(serializerName), "TrySerialize", toSerialize, currentPosition, bytesRemaining));

                yield return
                    If(BinOp(Expr(byteCounterName), CodeBinaryOperatorType.LessThanOrEqual, Expr("0")),
                        Return(failureReturnExpression));

                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, Expr(byteCounterName)));
                yield return Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, Expr(byteCounterName)));

                //yield return Stmt("Foo;");

            }

            private IEnumerable<CodeStatement> GenerateCustomDeserializeInstructions(
                CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining,
                CodeExpression toDeserialize, Type t)
            {
                string serializerName = this.GenerateTempVariableName("customSerializer");
                Type customSerializerType = this.format.GetCustomSerializationType(t);
                this.AddReferencedAssembly(customSerializerType.Assembly);

                yield return
                    Decl(serializerName, customSerializerType, new CodeObjectCreateExpression(customSerializerType));

                string byteCounterName = this.GenerateTempVariableName("bytesConsumed");

                yield return
                    Decl(byteCounterName, typeof(int),
                        Invoke(Expr(serializerName), "Deserialize", Out(toDeserialize), currentPosition, bytesRemaining));

                yield return Assign(currentPosition, new CodeBinaryOperatorExpression(currentPosition, CodeBinaryOperatorType.Add, Expr(byteCounterName)));
                yield return Assign(bytesRemaining, new CodeBinaryOperatorExpression(bytesRemaining, CodeBinaryOperatorType.Subtract, Expr(byteCounterName)));
            }

            private static bool IsTupleType(Type t)
            {
                return t.Assembly.Equals(typeof (System.Tuple<>).Assembly)
                       && t.Namespace != null
                       && t.Namespace.Equals(typeof (System.Tuple<>).Namespace)
                       && t.Name.StartsWith("Tuple`");
            }

            private IEnumerable<CodeStatement> GenerateTupleSerializeInstructions(
                CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining,
                CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                Type[] genericArgs = t.GetGenericArguments();
                
                Debug.Assert(genericArgs.Length <= 8);

                for (int i = 0; i < genericArgs.Length; ++i)
                {
                    string itemName = i < 7 ? string.Format("Item{0}", i + 1) : "Rest";
                    foreach (var stmt in  this.GenerateSerializeInstructions(currentPosition, bytesRemaining,
                        new CodePropertyReferenceExpression(toSerialize, itemName), genericArgs[i],
                        failureReturnExpression))
                        yield return stmt;
                }

            }

            private IEnumerable<CodeStatement> GenerateTupleDeserializeInstructions(
                CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining,
                CodeExpression toDeserialize, Type t)
            {
                Type[] genericArgs = t.GetGenericArguments();

                Debug.Assert(genericArgs.Length <= 8);

                CodeExpression[] tupleCreateArgs = new CodeExpression[genericArgs.Length];

                for (int i = 0; i < genericArgs.Length; ++i)
                {
                    string tempItemName = this.GenerateTempVariableName("tupleItem");
                    yield return Decl(tempItemName, genericArgs[i]);

                    var tempItem = Var(tempItemName);

                    tupleCreateArgs[i] = tempItem;

                    foreach (
                        var stmt in
                            this.GenerateDeserializeInstructions(currentPosition, bytesRemaining, tempItem,
                                genericArgs[i]))
                        yield return stmt;

                }

                yield return Assign(toDeserialize, new CodeObjectCreateExpression(t, tupleCreateArgs));

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
            /// </summary>
            /// <param name="t">The type whose generic parameters are to be evaluated.</param>
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

                List<CodeStatement> nullObjectStmts = new List<CodeStatement>();

                foreach (CodeStatement stmt in this.GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, new CodePrimitiveExpression(NULL_ARRAY_MARKER), typeof(int), failureReturnExpression))
                    nullObjectStmts.Add(stmt);

                List<CodeStatement> objectStmts = new List<CodeStatement>();

                string tempBufferVar = this.GenerateTempVariableName("tempBuffer");
                string formatterVar = this.GenerateTempVariableName("formatter");
                string memoryStreamVar = this.GenerateTempVariableName("memoryStream");

                objectStmts.Add(Decl(tempBufferVar, typeof(byte[])));
                
                objectStmts.Add(new CodeSnippetStatement(string.Format("using (System.IO.MemoryStream {0} = new System.IO.MemoryStream()) {{", memoryStreamVar)));

                objectStmts.Add(Decl(formatterVar, typeof(BinaryFormatter), new CodeObjectCreateExpression(typeof(BinaryFormatter))));
                
                objectStmts.Add(new CodeExpressionStatement(new CodeMethodInvokeExpression(Var(formatterVar), "Serialize", Var(memoryStreamVar), toSerialize)));

                objectStmts.Add(Assign(Var(tempBufferVar), new CodeMethodInvokeExpression(Var(memoryStreamVar), "ToArray")));

                objectStmts.Add(new CodeSnippetStatement("}"));

                foreach (CodeStatement stmt in this.GenerateArraySerializeInstructions(currentPosition, bytesRemaining, Var(tempBufferVar), typeof(byte[]), failureReturnExpression))
                    objectStmts.Add(stmt);

                if (this.minorVersion >= 2)
                {
                    yield return
                        IfElse(BinOp(toSerialize, CodeBinaryOperatorType.ValueEquality, new CodePrimitiveExpression(null)),
                            nullObjectStmts.ToArray(), objectStmts.ToArray());
                }
                else
                {
                    foreach (var stmt in objectStmts)
                        yield return stmt;
                }
            }

            private IEnumerable<CodeStatement> GenerateDotnetBinaryDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                Debug.Assert(t.IsSerializable);

                List<CodeStatement> objectStmts = new List<CodeStatement>();

                string tempBufferVar = this.GenerateTempVariableName("tempBuffer");
                string formatterVar = this.GenerateTempVariableName("formatter");
                string memoryStreamVar = this.GenerateTempVariableName("memoryStream");

                objectStmts.Add(Decl(tempBufferVar, typeof(byte[])));

                foreach (CodeStatement stmt in this.GenerateArrayDeserializeInstructions(currentPosition, bytesRemaining, Var(tempBufferVar), typeof(byte[])))
                    objectStmts.Add(stmt);

                objectStmts.Add(IfElse(new CodeBinaryOperatorExpression(Var(tempBufferVar), CodeBinaryOperatorType.ValueEquality, Expr("null")),
                    new CodeStatement[] { Assign(toDeserialize, Expr("null")) },
                    new CodeStatement[] {
                        new CodeSnippetStatement(string.Format("using (System.IO.MemoryStream {0} = new System.IO.MemoryStream({1})) {{", memoryStreamVar, tempBufferVar)),
                        Decl(formatterVar, typeof(BinaryFormatter), new CodeObjectCreateExpression(typeof(BinaryFormatter))),
                        Assign(toDeserialize, new CodeCastExpression(t, new CodeMethodInvokeExpression(Var(formatterVar), "Deserialize", Var(memoryStreamVar)))),
                        new CodeSnippetStatement("}")
                    }));

                foreach (var stmt in objectStmts)
                    yield return stmt;
            }

            private IEnumerable<CodeStatement> GenerateSerializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toSerialize, Type t, CodeExpression failureReturnExpression)
            {
                this.AddReferencedAssembly(t.Assembly);
                if (this.minorVersion >= 1 && this.format.HasCustomSerialization(t))
                    return GenerateCustomSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (this.minorVersion >= 1 && t == typeof (string))
                    return GenerateStringSerializeInstructions(currentPosition, bytesRemaining, toSerialize, failureReturnExpression);
                else if (this.minorVersion >= 1 && IsTupleType(t))
                    return GenerateTupleSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t, failureReturnExpression);
                else if (t.IsArray)
                    return GenerateArraySerializeInstructions(currentPosition, bytesRemaining, toSerialize, t,
                        failureReturnExpression);
                else if (t.IsEnum)
                    return GenerateEnumSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t,
                        failureReturnExpression);
                else if (t.IsPrimitive)
                    return GeneratePrimitiveSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t,
                        failureReturnExpression);
                else if (t.IsValueType && t.GetFields().All(x => x.IsPublic && !x.IsInitOnly))
                    return GenerateLegacyStructSerializeInstructions(currentPosition, bytesRemaining, toSerialize, t,
                        failureReturnExpression);
                else if (t.IsSerializable)
                    return GenerateDotnetBinarySerializeInstructions(currentPosition, bytesRemaining, toSerialize, t,
                        failureReturnExpression);
                else
                {
                    Logging.Error("Cannot generate serializer for type: {0}", t.FullName);
                    throw new NotImplementedException(string.Format("Cannot generate serializer for type: {0}",
                        t.FullName));
                }
            }

            private IEnumerable<CodeStatement> GenerateDeserializeInstructions(CodeVariableReferenceExpression currentPosition, CodeVariableReferenceExpression bytesRemaining, CodeExpression toDeserialize, Type t)
            {
                this.AddReferencedAssembly(t.Assembly);

                if (this.minorVersion >= 1 && this.format.HasCustomSerialization(t))
                    return GenerateCustomDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (this.minorVersion >= 1 && t == typeof(string))
                    return GenerateStringDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize);
                else if (this.minorVersion >= 1 && IsTupleType(t))
                    return GenerateTupleDeserializeInstructions(currentPosition, bytesRemaining, toDeserialize, t);
                else if (t.IsArray)
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
                    throw new NotImplementedException(string.Format("Cannot generate serializer for type: {0}",
                        t.FullName));
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
        internal class NaiadSerializationCodeGenerator : SerializationCodeGeneratorForType
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
                var cn = new CodeNamespace("Microsoft.Research.Naiad.Serialization.AutoGenerated");
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
                classDecl.Members.Add(GenerateTryDeserializeManyMethod(t));
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
            
            private CodeStatement CallSerialize(CodeExpression dest, CodeExpression value)
            {
                return IfElse(
                    Invoke(this.GeneratedClassName, "_Serialize", Ref(dest), value),
                    new CodeStatement[] { },
                    new CodeStatement[] { Return("false") });
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
                        AddType(field.FieldType);
                        yield return CallSerialize(dest, Field(value, field.Name));                     
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

                        AddType(field.FieldType);
                        yield return CallDeserialize(source, Field(value, field.Name));
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

            public CodeMemberMethod GenerateTryDeserializeManyMethod(Type t)
            {
                var m = new MethodHelper("TryDeserializeMany", typeof(int),
                    MemberAttributes.Final | MemberAttributes.Public);
                m.Param("source", typeof(RecvBuffer), FieldDirection.Ref);
                m.Param("target", typeof(ArraySegment<>).MakeGenericType(t));

                m.Add(Decl("numRead", typeof(int), Literal(0)));

                var forLoop = new CodeIterationStatement(Stmt("int i = 0"), Expr("i < target.Count"), Stmt("++i, ++numRead"));

                forLoop.Statements.Add(Stmt("bool success = this.TryDeserialize(ref source, out target.Array[i + target.Offset]);"));
                forLoop.Statements.Add(Stmt("if (!success) break;"));

                m.Add(forLoop);

                m.Return(Expr("numRead"));

                return m.Code;
            }

        }

        #endregion
    }
}
