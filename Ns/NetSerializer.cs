using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using static System.Buffers.ArrayPool<byte>;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Ns {
    /// <summary>
    /// Manages serialization to / deserialization from a stream
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "UnusedMethodReturnValue.Global")]
    public class NetSerializer {
        static NetSerializer() {
            #region Register primitives

            Converters[typeof(bool)] = ((s, o) => s.WriteU8((byte) ((bool) o ? 1 : 0)), s => s.ReadU8() != 0);
            Converters[typeof(byte)] = ((s, o) => s.WriteU8((byte) o), s => s.ReadU8());
            Converters[typeof(sbyte)] = ((s, o) => s.WriteS8((sbyte) o), s => s.ReadS8());
            Converters[typeof(ushort)] = ((s, o) => s.WriteU16((ushort) o), s => s.ReadU16());
            Converters[typeof(short)] = ((s, o) => s.WriteS16((short) o), s => s.ReadS16());
            Converters[typeof(uint)] = ((s, o) => s.WriteU32((uint) o), s => s.ReadU32());
            Converters[typeof(int)] = ((s, o) => s.WriteS32((int) o), s => s.ReadS32());
            Converters[typeof(ulong)] = ((s, o) => s.WriteU64((ulong) o), s => s.ReadU64());
            Converters[typeof(long)] = ((s, o) => s.WriteS64((long) o), s => s.ReadS64());
            Converters[typeof(float)] = ((s, o) => s.WriteSingle((float) o), s => s.ReadSingle());
            Converters[typeof(double)] = ((s, o) => s.WriteDouble((double) o), s => s.ReadDouble());
            Converters[typeof(decimal)] = ((s, o) => s.WriteDecimal((decimal) o), s => s.ReadDecimal());
            Converters[typeof(char)] = ((s, o) => s.WriteU16((char) o), s => (char) s.ReadU16());

            Converters[typeof(bool[])] = ((s, o) => {
                    var bA = (bool[]) o;
                    s.WriteS32(bA.Length);
                    s.WriteSpan<bool>(bA, bA.Length, sizeof(bool), false);
                },
                s => s.ReadArray<bool>(s.ReadS32(), sizeof(bool), false));

            Converters[typeof(byte[])] = ((s, o) => {
                    var u8A = (byte[]) o;
                    s.WriteS32(u8A.Length);
                    s.WriteSpan<byte>(u8A, u8A.Length, sizeof(byte), false);
                },
                s => s.ReadArray<byte>(s.ReadS32(), sizeof(byte), false));

            Converters[typeof(sbyte[])] = ((s, o) => {
                    var s8A = (sbyte[]) o;
                    s.WriteS32(s8A.Length);
                    s.WriteSpan<sbyte>(s8A, s8A.Length, sizeof(sbyte), false);
                },
                s => s.ReadArray<sbyte>(s.ReadS32(), sizeof(sbyte), false));

            Converters[typeof(ushort[])] = ((s, o) => {
                    var u16A = (ushort[]) o;
                    s.WriteS32(u16A.Length);
                    s.WriteSpan<ushort>(u16A, u16A.Length, sizeof(ushort), true);
                },
                s => s.ReadArray<ushort>(s.ReadS32(), sizeof(ushort), true));

            Converters[typeof(short[])] = ((s, o) => {
                    var s16A = (short[]) o;
                    s.WriteS32(s16A.Length);
                    s.WriteSpan<short>(s16A, s16A.Length, sizeof(short), true);
                },
                s => s.ReadArray<short>(s.ReadS32(), sizeof(short), true));

            Converters[typeof(uint[])] = ((s, o) => {
                var u32A = (uint[]) o;
                s.WriteS32(u32A.Length);
                s.WriteSpan<uint>(u32A, u32A.Length, sizeof(uint), true);
            }, s => s.ReadArray<uint>(s.ReadS32(), sizeof(uint), true));

            Converters[typeof(int[])] = ((s, o) => {
                var s32A = (int[]) o;
                s.WriteS32(s32A.Length);
                s.WriteSpan<int>(s32A, s32A.Length, sizeof(int), true);
            }, s => s.ReadArray<int>(s.ReadS32(), sizeof(int), true));

            Converters[typeof(ulong[])] = ((s, o) => {
                    var u64A = (ulong[]) o;
                    s.WriteS32(u64A.Length);
                    s.WriteSpan<ulong>(u64A, u64A.Length, sizeof(ulong), true);
                },
                s => s.ReadArray<ulong>(s.ReadS32(), sizeof(ulong), true));

            Converters[typeof(long[])] = ((s, o) => {
                var s64A = (long[]) o;
                s.WriteS32(s64A.Length);
                s.WriteSpan<long>(s64A, s64A.Length, sizeof(long), true);
            }, s => s.ReadArray<long>(s.ReadS32(), sizeof(long), true));

            Converters[typeof(float[])] = ((s, o) => {
                    var fA = (float[]) o;
                    s.WriteS32(fA.Length);
                    s.WriteSpan<float>(fA, fA.Length, sizeof(float), false);
                },
                s => s.ReadArray<float>(s.ReadS32(), sizeof(float), false));

            Converters[typeof(double[])] = ((s, o) => {
                    var dA = (double[]) o;
                    s.WriteS32(dA.Length);
                    s.WriteSpan<double>(dA, dA.Length, sizeof(double), false);
                },
                s => s.ReadArray<double>(s.ReadS32(), sizeof(double), false));

            Converters[typeof(decimal[])] = ((s, o) => {
                    var deA = (decimal[]) o;
                    s.WriteS32(deA.Length);
                    s.WriteSpan<decimal>(deA, deA.Length, sizeof(decimal), false);
                },
                s => s.ReadArray<decimal>(s.ReadS32(), sizeof(decimal), false));

            Converters[typeof(char[])] = ((s, o) => {
                    var cA = (char[]) o;
                    s.WriteS32(cA.Length);
                    s.WriteSpan<char>(cA, cA.Length, sizeof(char), false);
                },
                s => s.ReadArray<char>(s.ReadS32(), sizeof(char), false));

            Converters[typeof(List<bool>)] = ((s, o) => {
                    var bList = (List<bool>) o;
                    s.WriteS32(bList.Count);
                    foreach (var b in bList) s.WriteU8((byte) (b ? 1 : 0));
                },
                s => s.ReadList<bool>(s.ReadS32(), false));

            Converters[typeof(List<byte>)] = ((s, o) => {
                    var u8List = (List<byte>) o;
                    s.WriteS32(u8List.Count);
                    foreach (var u8 in u8List) s.WriteU8(u8);
                },
                s => s.ReadList<byte>(s.ReadS32(), false));

            Converters[typeof(List<sbyte>)] = ((s, o) => {
                    var s8List = (List<sbyte>) o;
                    s.WriteS32(s8List.Count);
                    foreach (var s8 in s8List) s.WriteS8(s8);
                },
                s => s.ReadList<sbyte>(s.ReadS32(), false));

            Converters[typeof(List<ushort>)] = ((s, o) => {
                    var u16List = (List<byte>) o;
                    s.WriteS32(u16List.Count);
                    foreach (var u16 in u16List) s.WriteU16(u16);
                },
                s => s.ReadList<ushort>(s.ReadS32(), true));

            Converters[typeof(List<short>)] = ((s, o) => {
                    var s16List = (List<short>) o;
                    s.WriteS32(s16List.Count);
                    foreach (var s16 in s16List) s.WriteS16(s16);
                },
                s => s.ReadList<short>(s.ReadS32(), true));

            Converters[typeof(List<uint>)] = ((s, o) => {
                    var u32List = (List<uint>) o;
                    s.WriteS32(u32List.Count);
                    foreach (var u32 in u32List) s.WriteU32(u32);
                },
                s => s.ReadList<uint>(s.ReadS32(), true));

            Converters[typeof(List<int>)] = ((s, o) => {
                    var s32List = (List<int>) o;
                    s.WriteS32(s32List.Count);
                    foreach (var s32 in s32List) s.WriteS32(s32);
                },
                s => s.ReadList<int>(s.ReadS32(), true));

            Converters[typeof(List<ulong>)] = ((s, o) => {
                    var u64List = (List<ulong>) o;
                    s.WriteS32(u64List.Count);
                    foreach (var u64 in u64List) s.WriteU64(u64);
                },
                s => s.ReadList<ulong>(s.ReadS32(), true));

            Converters[typeof(List<long>)] = ((s, o) => {
                    var s64List = (List<long>) o;
                    s.WriteS32(s64List.Count);
                    foreach (var s64 in s64List) s.WriteS64(s64);
                },
                s => s.ReadList<long>(s.ReadS32(), true));

            Converters[typeof(List<float>)] = ((s, o) => {
                    var fList = (List<float>) o;
                    s.WriteS32(fList.Count);
                    foreach (var f in fList) s.WriteSingle(f);
                },
                s => s.ReadList<float>(s.ReadS32(), false));

            Converters[typeof(List<double>)] = ((s, o) => {
                    var dList = (List<double>) o;
                    s.WriteS32(dList.Count);
                    foreach (var d in dList) s.WriteDouble(d);
                },
                s => s.ReadList<double>(s.ReadS32(), false));

            Converters[typeof(List<decimal>)] = ((s, o) => {
                    var deList = (List<decimal>) o;
                    s.WriteS32(deList.Count);
                    foreach (var de in deList) s.WriteDecimal(de);
                },
                s => s.ReadList<decimal>(s.ReadS32(), false));

            Converters[typeof(List<char>)] = ((s, o) => {
                    var cList = (List<char>) o;
                    s.WriteS32(cList.Count);
                    foreach (var c in cList) s.WriteU16(c);
                },
                s => s.ReadList<char>(s.ReadS32(), false));

            #endregion

            #region Register strings

            Converters[typeof(string)] = ((s, o) => s.WriteUtf8String((string) o),
                s => s.ReadUtf8String());

            Converters[typeof(string[])] = ((s, o) => {
                    var strArr = (string[]) o;
                    s.WriteS32(strArr.Length);
                    foreach (var str in strArr) {
                        s.WriteU8((byte) (str != null ? 1 : 0));
                        if (str != null)
                            s.WriteUtf8String(str);
                    }
                },
                s => {
                    var count = s.ReadS32();
                    var arr = new string[count];
                    for (var i = 0; i < count; i++) arr[i] = s.ReadU8() == 0 ? null : s.ReadUtf8String();

                    return arr;
                });

            Converters[typeof(List<string>)] = ((s, o) => {
                    var strList = (List<string>) o;
                    s.WriteS32(strList.Count);
                    foreach (var str in strList) {
                        s.WriteU8((byte) (str != null ? 1 : 0));
                        if (str != null)
                            s.WriteUtf8String(str);
                    }
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<string>(count);
                    for (var i = 0; i < count; i++) list.Add(s.ReadU8() == 0 ? null : s.ReadUtf8String());

                    return list;
                });

            #endregion

            #region Register misc collections

            Converters[typeof(Dictionary<string, string>)] = ((s, o) => {
                    var dict = (Dictionary<string, string>) o;
                    s.WriteS32(dict.Count);
                    foreach (var kvp in dict) {
                        var key = kvp.Key;
                        var value = kvp.Value;
                        s.WriteU8((byte) (key != null ? 1 : 0));
                        if (key != null)
                            s.WriteUtf8String(key);
                        s.WriteU8((byte) (value != null ? 1 : 0));
                        if (value != null)
                            s.WriteUtf8String(value);
                    }
                },
                s => {
                    var dict = new Dictionary<string, string>();
                    var count = s.ReadS32();
                    for (var i = 0; i < count; i++) {
                        var k = s.ReadU8() == 0 ? null : s.ReadUtf8String();
                        var v = s.ReadU8() == 0 ? null : s.ReadUtf8String();
                        if (k != null) dict[k] = v;
                    }

                    return dict;
                });

            #endregion
        }

        private static readonly
            Dictionary<Type, (Action<NetSerializer, object> encoder, Func<NetSerializer, object> decoder)> Converters =
                new Dictionary<Type, (Action<NetSerializer, object> encoder, Func<NetSerializer, object> decoder)>();

        /// <summary>
        /// Register a type's encoder and decoder
        /// </summary>
        /// <param name="encoder">Encoder for object to stream</param>
        /// <param name="decoder">Decoder for stream to object</param>
        /// <param name="generateCollectionConverters">Generate converters for basic collections</param>
        public static void Register<T>(Action<NetSerializer, object> encoder, Func<NetSerializer, object> decoder,
            bool generateCollectionConverters = true) {
            var type = typeof(T);
            Converters[type] = (encoder, decoder);
            if (!generateCollectionConverters) return;
            var nc = !type.IsValueType;
            Converters[typeof(T[])] = ((s, o) => {
                var arr = (T[]) o;
                s.WriteS32(arr.Length);
                foreach (var e in arr) s.Serialize(e, nc, encoder);
            }, s => {
                var count = s.ReadS32();
                var res = new T[count];
                for (var i = 0; i < count; i++) res[i] = s.Deserialize<T>(nc, decoder);

                return res;
            });
            Converters[typeof(List<T>)] = ((s, o) => {
                var list = (List<T>) o;
                s.WriteS32(list.Count);
                foreach (var e in list) s.Serialize(e, nc, encoder);
            }, s => {
                var count = s.ReadS32();
                var res = new List<T>(count);
                for (var i = 0; i < count; i++) res.Add(s.Deserialize<T>(nc, decoder));
                return res;
            });
            Converters[typeof(Dictionary<string, T>)] = ((s, o) => {
                    var dict = (Dictionary<string, T>) o;
                    s.WriteS32(dict.Count);
                    foreach (var kvp in dict) {
                        var key = kvp.Key;
                        s.WriteU8((byte) (key != null ? 1 : 0));
                        if (key != null)
                            s.WriteUtf8String(key);
                        s.Serialize(kvp.Value, nc, encoder);
                    }
                },
                s => {
                    var dict = new Dictionary<string, T>();
                    var count = s.ReadS32();
                    for (var i = 0; i < count; i++) {
                        var k = s.ReadU8() == 0 ? null : s.ReadUtf8String();
                        var v = s.Deserialize<T>(nc, decoder);
                        if (k != null) dict[k] = v;
                    }

                    return dict;
                });
        }

        /// <summary>
        /// Register dictionary type
        /// </summary>
        /// <typeparam name="TKey">Dictionary key type</typeparam>
        /// <typeparam name="TValue">Dictionary value type</typeparam>
        /// <exception cref="ApplicationException">If unregistered types are used</exception>
        public static void AddDictionary<TKey, TValue>() {
            if (!Converters.TryGetValue(typeof(TKey), out var key))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary key type is not already registered");
            if (!Converters.TryGetValue(typeof(TValue), out var value))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary value type is not already registered");
            var keyE = key.encoder;
            var keyD = key.decoder;
            var valueE = value.encoder;
            var valueD = value.decoder;
            var ncKey = !typeof(TKey).IsValueType;
            var ncValue = !typeof(TValue).IsValueType;
            Converters[typeof(Dictionary<TKey, TValue>)] = ((s, o) => {
                    var dict = (Dictionary<TKey, TValue>) o;
                    s.WriteS32(dict.Count);
                    foreach (var kvp in dict) {
                        s.Serialize(kvp.Key, ncKey, keyE);
                        s.Serialize(kvp.Value, ncValue, valueE);
                    }
                },
                s => {
                    var dict = new Dictionary<TKey, TValue>();
                    var count = s.ReadS32();
                    for (var i = 0; i < count; i++) {
                        var k = s.Deserialize<TKey>(ncKey, keyD);
                        var v = s.Deserialize<TValue>(ncValue, valueD);
                        dict[k] = v;
                    }

                    return dict;
                });
        }

        /// <summary>
        /// Get encoder/decoder pair for type
        /// </summary>
        /// <param name="res">Encoder and decoder</param>
        /// <typeparam name="T">Type to retrieve encoder/decoder for</typeparam>
        /// <returns>True if encoder/decoder were obtained</returns>
        public static bool GetConverter<T>(
            out (Action<NetSerializer, object> encoder, Func<NetSerializer, object> decoder) res)
            => Converters.TryGetValue(typeof(T), out res);

        /// <summary>
        /// Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj) {
            var type = typeof(T);
            if (!Converters.TryGetValue(type, out var res))
                throw new ApplicationException("Tried to serialize unregistered type");
            Serialize(obj, !type.IsValueType, res.encoder);
        }

        /// <summary>
        /// Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <param name="nullCheck">Serialize null state</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj, bool nullCheck) {
            if (!Converters.TryGetValue(typeof(T), out var res))
                throw new ApplicationException("Tried to serialize unregistered type");
            Serialize(obj, nullCheck, res.encoder);
        }

        /// <summary>
        /// Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <param name="nullCheck">Serialize null state</param>
        /// <param name="encoder">Encoder to use</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj, bool nullCheck, Action<NetSerializer, object> encoder) {
            if (nullCheck) {
                WriteU8((byte) (obj != null ? 1 : 0));
                if (obj == null) return;
            }

            encoder.Invoke(this, obj);
        }

        /// <summary>
        /// Deserialize an object
        /// </summary>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>() {
            var type = typeof(T);
            if (!Converters.TryGetValue(type, out var res))
                throw new ApplicationException("Tried to deserialize unregistered type");
            return Deserialize<T>(!type.IsValueType, res.decoder);
        }

        /// <summary>
        /// Deserialize an object
        /// </summary>
        /// <param name="nullCheck">Deserialize null state</param>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>(bool nullCheck) {
            if (!Converters.TryGetValue(typeof(T), out var res))
                throw new ApplicationException("Tried to deserialize unregistered type");
            return Deserialize<T>(nullCheck, res.decoder);
        }

        /// <summary>
        /// Deserialize an object
        /// </summary>
        /// <param name="nullCheck">Deserialize null state</param>
        /// <param name="decoder">Decoder to use</param>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>(bool nullCheck, Func<NetSerializer, object> decoder) {
            if (nullCheck && ReadU8() == 0) return default;
            return (T) decoder.Invoke(this);
        }

        private static readonly bool Swap = BitConverter.IsLittleEndian;

        /// <summary>
        /// Stream this instance wraps
        /// </summary>
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public Stream BaseStream { get; set; }

        private readonly byte[] _buffer = new byte[sizeof(decimal)];
        private Decoder Decoder => _decoder ??= Encoding.UTF8.GetDecoder();
        private Decoder _decoder;
        private Encoder Encoder => _encoder ??= Encoding.UTF8.GetEncoder();
        private Encoder _encoder;

        /// <summary>
        /// Create new NetSerializer instance
        /// </summary>
        /// <param name="baseStream">Stream to wrap</param>
        public NetSerializer(Stream baseStream) {
            BaseStream = baseStream;
        }

        /// <summary>
        /// Read array
        /// </summary>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <param name="target">Optional existing array to operate on</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <returns>Array</returns>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public T[] ReadArray<T>(int count, int order, bool enableSwap, T[] target = null) where T : unmanaged {
            target ??= new T[count];
            ReadSpan<T>(target, count, order, enableSwap);
            return target;
        }

        /// <summary>
        /// Read list
        /// </summary>
        /// <param name="count">Number of elements</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <param name="target">Optional existing array to operate on</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <returns>Array</returns>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public unsafe List<T> ReadList<T>(int count, bool enableSwap, List<T> target = null) where T : unmanaged {
            target ??= new List<T>();
            if (target.Capacity < count)
                target.Capacity = count;
            var order = sizeof(T);
            var mainLen = count * order;
            var buf = Shared.Rent(4096);
            var span = buf.AsSpan();
            try {
                fixed (byte* p = &span.GetPinnableReference()) {
                    int left = mainLen, read, tot = 0, curTot = 0;
                    if (order == 1 || !enableSwap || !Swap) {
                        do {
                            read = BaseStream.Read(buf, curTot, Math.Min(4096 - curTot, left));
                            curTot += read;
                            var trunc = curTot - curTot % order;
                            for (var i = 0; i < trunc; i += order)
                                target.Add(*(T*) (p + i));
                            curTot -= trunc;
                            span.Slice(trunc, curTot).CopyTo(span);
                            left -= read;
                            tot += read;
                        } while (left > 0);

                        if (left > 0)
                            throw new ApplicationException(
                                $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left, 0x{BaseStream.Position:X} end position");
                        return target;
                    }

                    do {
                        read = BaseStream.Read(buf, curTot, Math.Min(4096 - curTot, left));
                        curTot += read;
                        var trunc = curTot - curTot % order;
                        switch (order) {
                            case 2:
                                for (var i = 0; i < trunc; i += 2) {
                                    var tmp = p[i];
                                    p[i] = p[i + 1];
                                    p[i + 1] = tmp;
                                    target.Add(*(T*) (p + i));
                                }

                                break;
                            case 4:
                                for (var i = 0; i < trunc; i += 4) {
                                    var tmp = p[i];
                                    p[i] = p[i + 3];
                                    p[i + 3] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 2];
                                    p[i + 2] = tmp;
                                    target.Add(*(T*) (p + i));
                                }

                                break;
                            case 8:
                                for (var i = 0; i < trunc; i += 8) {
                                    var tmp = p[i];
                                    p[i] = p[i + 7];
                                    p[i + 7] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 6];
                                    p[i + 6] = tmp;
                                    tmp = p[i + 2];
                                    p[i + 2] = p[i + 5];
                                    p[i + 5] = tmp;
                                    tmp = p[i + 3];
                                    p[i + 3] = p[i + 4];
                                    p[i + 4] = tmp;
                                    target.Add(*(T*) (p + i));
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < trunc; i += order) {
                                    for (var j = 0; j < half; j++) {
                                        var tmp = p[i];
                                        var sec = i + order - 1 - j;
                                        p[i] = p[sec];
                                        p[sec] = tmp;
                                        target.Add(*(T*) (p + i));
                                    }
                                }

                                break;
                        }

                        curTot -= trunc;
                        span.Slice(trunc, curTot).CopyTo(span);
                        left -= read;
                        tot += read;
                    } while (left > 0);

                    if (left > 0)
                        throw new ApplicationException(
                            $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left, 0x{BaseStream.Position:X} end position");
                    return target;
                }
            }
            finally {
                Shared.Return(buf);
            }
        }

        /// <summary>
        /// Read span
        /// </summary>
        /// <param name="target">Target buffer</param>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public unsafe void ReadSpan<T>(Span<T> target, int count, int order, bool enableSwap) where T : unmanaged {
            var mainTarget = MemoryMarshal.Cast<T, byte>(target);
            var mainLen = count * order;
            var buf = Shared.Rent(4096);
            var span = buf.AsSpan();
            try {
                int left = mainLen, tot = 0;
                do {
                    var read = BaseStream.Read(buf, 0, Math.Min(4096, left));
                    span.Slice(0, read).CopyTo(mainTarget.Slice(tot));
                    left -= read;
                    tot += read;
                } while (left > 0);

                if (left > 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left, 0x{BaseStream.Position:X} end position");

                if (order != 1 && enableSwap && Swap) {
                    fixed (byte* p = &mainTarget.GetPinnableReference()) {
                        switch (order) {
                            case 2:
                                for (var i = 0; i < mainLen; i += 2) {
                                    var tmp = p[i];
                                    p[i] = p[i + 1];
                                    p[i + 1] = tmp;
                                }

                                break;
                            case 4:
                                for (var i = 0; i < mainLen; i += 4) {
                                    var tmp = p[i];
                                    p[i] = p[i + 3];
                                    p[i + 3] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 2];
                                    p[i + 2] = tmp;
                                }

                                break;
                            case 8:
                                for (var i = 0; i < mainLen; i += 8) {
                                    var tmp = p[i];
                                    p[i] = p[i + 7];
                                    p[i + 7] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 6];
                                    p[i + 6] = tmp;
                                    tmp = p[i + 2];
                                    p[i + 2] = p[i + 5];
                                    p[i + 5] = tmp;
                                    tmp = p[i + 3];
                                    p[i + 3] = p[i + 4];
                                    p[i + 4] = tmp;
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < mainLen; i += order) {
                                    for (var j = 0; j < half; j++) {
                                        var tmp = p[i];
                                        var sec = i + order - 1 - j;
                                        p[i] = p[sec];
                                        p[sec] = tmp;
                                    }
                                }

                                break;
                        }
                    }
                }
            }
            finally {
                Shared.Return(buf);
            }
        }

        /// <summary>
        /// Write span
        /// </summary>
        /// <param name="source">Source buffer</param>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        public unsafe void WriteSpan<T>(Span<T> source, int count, int order, bool enableSwap) where T : unmanaged {
            var mainTarget = MemoryMarshal.Cast<T, byte>(source);
            var buf = Shared.Rent(4096);
            var span = buf.AsSpan();
            var left = count * order;
            var tot = 0;
            try {
                if (order == 1 || !enableSwap || !Swap) {
                    while (left > 0) {
                        var noSwapCur = Math.Min(left, 4096);
                        mainTarget.Slice(tot, noSwapCur).CopyTo(buf);
                        BaseStream.Write(buf, 0, noSwapCur);
                        left -= noSwapCur;
                        tot += noSwapCur;
                    }

                    return;
                }

                var maxCount = 4096 / order * order;
                fixed (byte* p = &span.GetPinnableReference()) {
                    while (left != 0) {
                        var cur = Math.Min(left, maxCount);
                        mainTarget.Slice(tot, cur).CopyTo(buf);
                        switch (order) {
                            case 2:
                                for (var i = 0; i < cur; i += 2) {
                                    var tmp = p[i];
                                    p[i] = p[i + 1];
                                    p[i + 1] = tmp;
                                }

                                break;
                            case 4:
                                for (var i = 0; i < cur; i += 4) {
                                    var tmp = p[i];
                                    p[i] = p[i + 3];
                                    p[i + 3] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 2];
                                    p[i + 2] = tmp;
                                }

                                break;
                            case 8:
                                for (var i = 0; i < cur; i += 8) {
                                    var tmp = p[i];
                                    p[i] = p[i + 7];
                                    p[i + 7] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 6];
                                    p[i + 6] = tmp;
                                    tmp = p[i + 2];
                                    p[i + 2] = p[i + 5];
                                    p[i + 5] = tmp;
                                    tmp = p[i + 3];
                                    p[i + 3] = p[i + 4];
                                    p[i + 4] = tmp;
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < cur; i += order) {
                                    for (var j = 0; j < half; j++) {
                                        var tmp = p[i];
                                        var sec = i + order - 1 - j;
                                        p[i] = p[sec];
                                        p[sec] = tmp;
                                    }
                                }

                                break;
                        }

                        BaseStream.Write(buf, 0, cur);
                        left -= cur;
                        tot += cur;
                    }
                }
            }
            finally {
                Shared.Return(buf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> ReadBase(int length) {
            var tot = 0;
            do {
                var read = BaseStream.Read(_buffer, tot, length - tot);
                if (read == 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{length:X} left, 0x{BaseStream.Position:X} end position");
                tot += read;
            } while (tot < length);

            return _buffer;
        }

        /// <summary>
        /// Read UTF-8 string
        /// </summary>
        /// <returns>Decoded string</returns>
        public unsafe string ReadUtf8String() {
            Decoder.Reset();
            var b = new StringBuilder();
            var tmpBuf = Shared.Rent(4096);
            try {
                fixed (byte* tmpBufPtr = tmpBuf) {
                    var charPtr = (char*) (tmpBufPtr + 2048);
                    int len;
                    while ((len = Read7S32(out _)) != 0) {
                        int read, tot = 0;
                        do {
                            read = BaseStream.Read(tmpBuf, 0, Math.Min(len, 2048));
                            len -= read;
                            var cur = 0;
                            do {
                                Decoder.Convert(tmpBufPtr + cur, read - cur, charPtr, 2048 / sizeof(char),
                                    false, out var numIn, out var numOut, out _);
                                b.Append(charPtr, numOut);
                                cur += numIn;
                            } while (cur != read);

                            tot += read;
                        } while (len > 0 && read != 0);

                        if (len > 0)
                            throw new ApplicationException(
                                $"Failed to read required number of bytes! 0x{tot:X} read, 0x{tot:X} left, 0x{BaseStream.Position:X} end position");
                    }
                }
            }
            finally {
                Shared.Return(tmpBuf);
                //ArrayPool<byte>.Shared.Return(tmpBufOut);
            }

            return b.ToString();
        }

        /// <summary>
        /// Write UTF-8 string
        /// </summary>
        /// <param name="value">String to write</param>
        public unsafe void WriteUtf8String(string value) {
            Encoder.Reset();
            var tmpBuf = Shared.Rent(4096);
            try {
                fixed (char* strPtr = value) {
                    fixed (byte* tmpBufPtr = tmpBuf) {
                        var vStringOfs = 0;
                        var vStringLeft = value.Length;
                        while (vStringLeft > 0) {
                            Encoder.Convert(strPtr + vStringOfs, vStringLeft, tmpBufPtr, 4096, false,
                                out var numIn, out var numOut, out _);
                            vStringOfs += numIn;
                            vStringLeft -= numIn;
                            Write7S32(numOut);
                            BaseStream.Write(tmpBuf, 0, numOut);
                        }
                    }
                }
            }
            finally {
                Shared.Return(tmpBuf);
            }

            Write7S32(0);
        }

        /// <summary>
        /// Decode 32-bit integer
        /// </summary>
        /// <param name="len">Length of decoded value</param>
        /// <returns>Decoded value</returns>
        /// <exception cref="EndOfStreamException">When end of stream was prematurely reached</exception>
        public int Read7S32(out int len) {
            len = 1;
            var bits = 6;
            var flag = false;
            var c = BaseStream.Read(_buffer, 0, 1);
            if (c == 0)
                throw new EndOfStreamException();
            var v = _buffer[0];
            var value = v & 0x3f;
            if ((v & 0x40) != 0)
                flag = true;
            if (v <= 0x7f)
                while (bits < sizeof(int) * 8) {
                    var c2 = BaseStream.Read(_buffer, 0, 1);
                    if (c2 == 0)
                        throw new EndOfStreamException();
                    var v2 = _buffer[0];
                    value |= (v2 & 0x7f) << bits;
                    len++;
                    bits += 7;
                    if (v2 > 0x7f)
                        break;
                }

            if (flag)
                value = ~value;

            return value;
        }

        /// <summary>
        /// Encode 32-bit integer
        /// </summary>
        /// <param name="value">Value to encode</param>
        /// <returns>Length of encoded value</returns>
        public int Write7S32(int value) {
            byte flag = 0;
            if (value < 0) {
                value = ~value;
                flag = 0x40;
            }

            var uValue = (uint) value;
            var len = 1;
            var ofs = 0;
            if (uValue < 0x40)
                _buffer[ofs++] = (byte) (0x80 | (uValue & 0x3f) | flag);
            else
                _buffer[ofs++] = (byte) ((uValue & 0x3f) | flag);
            uValue >>= 6;
            while (uValue != 0) {
                if (uValue < 0x80)
                    _buffer[ofs++] = (byte) (0x80 | (uValue & 0x7f));
                else
                    _buffer[ofs++] = (byte) (uValue & 0x7f);
                len++;
                uValue >>= 7;
            }

            BaseStream.Write(_buffer, 0, ofs);

            return len;
        }

        /// <summary>
        /// Decode 64-bit integer
        /// </summary>
        /// <param name="len">Length of decoded value</param>
        /// <returns>Decoded value</returns>
        /// <exception cref="EndOfStreamException">When end of stream was prematurely reached</exception>
        public long Read7S64(out int len) {
            len = 1;
            var bits = 6;
            var flag = false;
            var c = BaseStream.Read(_buffer, 0, 1);
            if (c == 0)
                throw new EndOfStreamException();
            var v = _buffer[0];
            long value = v & 0x3f;
            if ((v & 0x40) != 0)
                flag = true;
            if (v <= 0x7f)
                while (bits < sizeof(long) * 8) {
                    var c2 = BaseStream.Read(_buffer, 0, 1);
                    if (c2 == 0)
                        throw new EndOfStreamException();
                    var v2 = _buffer[0];
                    value |= (long) (v2 & 0x7f) << bits;
                    len++;
                    bits += 7;
                    if (v2 > 0x7f)
                        break;
                }

            if (flag)
                value = ~value;

            return value;
        }

        /// <summary>
        /// Encode 64-bit integer
        /// </summary>
        /// <param name="value">Value to encode</param>
        /// <returns>Length of encoded value</returns>
        public int Write7S64(long value) {
            byte flag = 0;
            if (value < 0) {
                value = ~value;
                flag = 0x40;
            }

            var uValue = (ulong) value;
            var len = 1;
            var ofs = 0;
            if (uValue < 0x40)
                _buffer[ofs++] = (byte) (0x80 | (uValue & 0x3f) | flag);
            else
                _buffer[ofs++] = (byte) ((uValue & 0x3f) | flag);
            uValue >>= 6;
            while (uValue != 0) {
                if (uValue < 0x80)
                    _buffer[ofs++] = (byte) (0x80 | (uValue & 0x7f));
                else
                    _buffer[ofs++] = (byte) (uValue & 0x7f);
                len++;
                uValue >>= 7;
            }

            BaseStream.Write(_buffer, 0, ofs);

            return len;
        }

        /// <summary>
        /// Read signed 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public sbyte ReadS8() {
            ReadBase(sizeof(byte));
            return (sbyte) _buffer[0];
        }

        /// <summary>
        /// Write signed 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS8(sbyte value) {
            _buffer[0] = (byte) value;
            BaseStream.Write(_buffer, 0, sizeof(byte));
        }

        /// <summary>
        /// Read unsigned 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public byte ReadU8() {
            ReadBase(sizeof(byte));
            return _buffer[0];
        }

        /// <summary>
        /// Write unsigned 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU8(byte value) {
            _buffer[0] = value;
            BaseStream.Write(_buffer, 0, sizeof(byte));
        }

        /// <summary>
        /// Read signed 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public short ReadS16() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<short>(ReadBase(sizeof(short))))
                : MemoryMarshal.Read<short>(ReadBase(sizeof(short)));

        /// <summary>
        /// Write signed 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS16(short value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(short));
        }

        /// <summary>
        /// Read unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public ushort ReadU16() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<ushort>(ReadBase(sizeof(ushort))))
                : MemoryMarshal.Read<ushort>(ReadBase(sizeof(ushort)));

        /// <summary>
        /// Write unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU16(ushort value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(ushort));
        }

        /// <summary>
        /// Read signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public int ReadS32() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<int>(ReadBase(sizeof(int))))
                : MemoryMarshal.Read<int>(ReadBase(sizeof(int)));

        /// <summary>
        /// Write signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS32(int value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(int));
        }

        /// <summary>
        /// Read unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public uint ReadU32() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<uint>(ReadBase(sizeof(uint))))
                : MemoryMarshal.Read<uint>(ReadBase(sizeof(uint)));

        /// <summary>
        /// Write unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU32(uint value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(uint));
        }

        /// <summary>
        /// Read signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public long ReadS64() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<long>(ReadBase(sizeof(long))))
                : MemoryMarshal.Read<long>(ReadBase(sizeof(long)));

        /// <summary>
        /// Write signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS64(long value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(long));
        }

        /// <summary>
        /// Read unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public ulong ReadU64() =>
            Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<ulong>(ReadBase(sizeof(ulong))))
                : MemoryMarshal.Read<ulong>(ReadBase(sizeof(ulong)));

        /// <summary>
        /// Write unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU64(ulong value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(ulong));
        }

        /// <summary>
        /// Read single-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe float ReadSingle() {
            ReadBase(sizeof(float));
            fixed (byte* buffer = _buffer)
                return *(float*) buffer;
        }

        /// <summary>
        /// Write single-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteSingle(float value) {
            fixed (byte* buffer = _buffer) *(float*) buffer = value;
            BaseStream.Write(_buffer, 0, sizeof(float));
        }

        /// <summary>
        /// Read double-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe double ReadDouble() {
            ReadBase(sizeof(double));
            fixed (byte* buffer = _buffer)
                return *(double*) buffer;
        }

        /// <summary>
        /// Write double-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteDouble(double value) {
            fixed (byte* buffer = _buffer) *(double*) buffer = value;
            BaseStream.Write(_buffer, 0, sizeof(double));
        }

        /// <summary>
        /// Read decimal value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe decimal ReadDecimal() {
            ReadBase(sizeof(decimal));
            fixed (byte* buffer = _buffer)
                return *(decimal*) buffer;
        }

        /// <summary>
        /// Write decimal value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteDecimal(decimal value) {
            fixed (byte* buffer = _buffer) *(decimal*) buffer = value;
            BaseStream.Write(_buffer, 0, sizeof(decimal));
        }
    }
}