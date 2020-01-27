using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

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

            Register<byte>((s, o) => s.WriteU8((byte) o), s => s.ReadU8());
            Register<sbyte>((s, o) => s.WriteS8((sbyte) o), s => s.ReadS8());
            Register<ushort>((s, o) => s.WriteU16((ushort) o), s => s.ReadU16());
            Register<short>((s, o) => s.WriteS16((short) o), s => s.ReadS16());
            Register<uint>((s, o) => s.WriteU32((uint) o), s => s.ReadU32());
            Register<int>((s, o) => s.WriteS32((int) o), s => s.ReadS32());
            Register<ulong>((s, o) => s.WriteU64((ulong) o), s => s.ReadU64());
            Register<long>((s, o) => s.WriteS64((long) o), s => s.ReadS64());
            Register<float>((s, o) => s.WriteSingle((float) o), s => s.ReadSingle());
            Register<double>((s, o) => s.WriteDouble((double) o), s => s.ReadDouble());
            Register<decimal>((s, o) => s.WriteDecimal((decimal) o), s => s.ReadDecimal());
            Register<char>((s, o) => s.WriteU16((char) o), s => (char) s.ReadU16());

            Register<byte[]>((s, o) => {
                    var u8A = (byte[]) o;
                    s.WriteS32(u8A.Length);
                    s.WriteSpan<byte>(u8A, u8A.Length, sizeof(byte), false);
                },
                s => s.ReadArray<byte>(s.ReadS32(), sizeof(byte), false));

            Register<sbyte[]>((s, o) => {
                    var s8A = (sbyte[]) o;
                    s.WriteS32(s8A.Length);
                    s.WriteSpan<sbyte>(s8A, s8A.Length, sizeof(sbyte), false);
                },
                s => s.ReadArray<sbyte>(s.ReadS32(), sizeof(sbyte), false));

            Register<ushort[]>((s, o) => {
                    var u16A = (ushort[]) o;
                    s.WriteS32(u16A.Length);
                    s.WriteSpan<ushort>(u16A, u16A.Length, sizeof(ushort), true);
                },
                s => s.ReadArray<ushort>(s.ReadS32(), sizeof(ushort), true));

            Register<short[]>((s, o) => {
                    var s16A = (short[]) o;
                    s.WriteS32(s16A.Length);
                    s.WriteSpan<short>(s16A, s16A.Length, sizeof(short), true);
                },
                s => s.ReadArray<short>(s.ReadS32(), sizeof(short), true));

            Register<uint[]>((s, o) => {
                var u32A = (uint[]) o;
                s.WriteS32(u32A.Length);
                s.WriteSpan<uint>(u32A, u32A.Length, sizeof(uint), true);
            }, s => s.ReadArray<uint>(s.ReadS32(), sizeof(uint), true));

            Register<int[]>((s, o) => {
                var s32A = (int[]) o;
                s.WriteS32(s32A.Length);
                s.WriteSpan<int>(s32A, s32A.Length, sizeof(int), true);
            }, s => s.ReadArray<int>(s.ReadS32(), sizeof(int), true));

            Register<ulong[]>((s, o) => {
                    var u64A = (ulong[]) o;
                    s.WriteS32(u64A.Length);
                    s.WriteSpan<ulong>(u64A, u64A.Length, sizeof(ulong), true);
                },
                s => s.ReadArray<ulong>(s.ReadS32(), sizeof(ulong), true));

            Register<long[]>((s, o) => {
                var s64A = (long[]) o;
                s.WriteS32(s64A.Length);
                s.WriteSpan<long>(s64A, s64A.Length, sizeof(long), true);
            }, s => s.ReadArray<long>(s.ReadS32(), sizeof(long), true));

            Register<float[]>((s, o) => {
                    var fA = (float[]) o;
                    s.WriteS32(fA.Length);
                    s.WriteSpan<float>(fA, fA.Length, sizeof(float), false);
                },
                s => s.ReadArray<float>(s.ReadS32(), sizeof(float), false));

            Register<double[]>((s, o) => {
                    var dA = (double[]) o;
                    s.WriteS32(dA.Length);
                    s.WriteSpan<double>(dA, dA.Length, sizeof(double), false);
                },
                s => s.ReadArray<double>(s.ReadS32(), sizeof(double), false));

            Register<decimal[]>((s, o) => {
                    var deA = (decimal[]) o;
                    s.WriteS32(deA.Length);
                    s.WriteSpan<decimal>(deA, deA.Length, sizeof(decimal), false);
                },
                s => s.ReadArray<decimal>(s.ReadS32(), sizeof(decimal), false));

            Register<char[]>((s, o) => {
                    var cA = (char[]) o;
                    s.WriteS32(cA.Length);
                    s.WriteSpan<char>(cA, cA.Length, sizeof(char), false);
                },
                s => s.ReadArray<char>(s.ReadS32(), sizeof(char), false));

            Register<List<byte>>((s, o) => {
                    var u8List = (List<byte>) o;
                    s.WriteS32(u8List.Count);
                    foreach (var u8 in u8List) s.WriteU8(u8);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<byte> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadU8());

                    return list;
                });

            Register<List<sbyte>>((s, o) => {
                    var s8List = (List<sbyte>) o;
                    s.WriteS32(s8List.Count);
                    foreach (var s8 in s8List) s.WriteS8(s8);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<sbyte> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadS8());

                    return list;
                });

            Register<List<ushort>>((s, o) => {
                    var u16List = (List<byte>) o;
                    s.WriteS32(u16List.Count);
                    foreach (var u16 in u16List) s.WriteU16(u16);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<ushort> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadU16());

                    return list;
                });

            Register<List<short>>((s, o) => {
                    var s16List = (List<short>) o;
                    s.WriteS32(s16List.Count);
                    foreach (var s16 in s16List) s.WriteS16(s16);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<short> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadS16());

                    return list;
                });

            Register<List<uint>>((s, o) => {
                    var u32List = (List<uint>) o;
                    s.WriteS32(u32List.Count);
                    foreach (var u32 in u32List) s.WriteU32(u32);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<uint> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadU32());

                    return list;
                });

            Register<List<int>>((s, o) => {
                    var s32List = (List<int>) o;
                    s.WriteS32(s32List.Count);
                    foreach (var s32 in s32List) s.WriteS32(s32);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<int> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadS32());

                    return list;
                });

            Register<List<ulong>>((s, o) => {
                    var u64List = (List<ulong>) o;
                    s.WriteS32(u64List.Count);
                    foreach (var u64 in u64List) s.WriteU64(u64);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<ulong> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadU64());

                    return list;
                });

            Register<List<long>>((s, o) => {
                    var s64List = (List<long>) o;
                    s.WriteS32(s64List.Count);
                    foreach (var s64 in s64List) s.WriteS64(s64);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<long> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadS64());

                    return list;
                });

            Register<List<float>>((s, o) => {
                    var fList = (List<float>) o;
                    s.WriteS32(fList.Count);
                    foreach (var f in fList) s.WriteSingle(f);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<float> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadSingle());

                    return list;
                });

            Register<List<double>>((s, o) => {
                    var dList = (List<double>) o;
                    s.WriteS32(dList.Count);
                    foreach (var d in dList) s.WriteDouble(d);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<double> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadDouble());

                    return list;
                });

            Register<List<decimal>>((s, o) => {
                    var deList = (List<decimal>) o;
                    s.WriteS32(deList.Count);
                    foreach (var de in deList) s.WriteDecimal(de);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<decimal> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadDecimal());

                    return list;
                });

            Register<List<char>>((s, o) => {
                    var cList = (List<char>) o;
                    s.WriteS32(cList.Count);
                    foreach (var c in cList) s.WriteU16(c);
                },
                s => {
                    var count = s.ReadS32();
                    var list = new List<char> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add((char) s.ReadU16());

                    return list;
                });

            #endregion

            #region Register strings

            Register<string>((s, o) => s.WriteUtf8String((string) o),
                s => s.ReadUtf8String());

            Register<string[]>((s, o) => {
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

            Register<List<string>>((s, o) => {
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
                    var list = new List<string> {Capacity = count};
                    for (var i = 0; i < count; i++) list.Add(s.ReadU8() == 0 ? null : s.ReadUtf8String());

                    return list;
                });

            #endregion

            #region Register misc collections

            Register<Dictionary<string, string>>((s, o) => {
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
        public static void Register<T>(Action<NetSerializer, object> encoder, Func<NetSerializer, object> decoder) {
            var type = typeof(T);
            Converters[type] = (encoder, decoder);
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
                var res = new List<T> {Capacity = count};
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
        /// 
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <exception cref="ApplicationException"></exception>
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
            Register<Dictionary<TKey, TValue>>((s, o) => {
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

        private static readonly bool Swap = !BitConverter.IsLittleEndian;

        /// <summary>
        /// Stream this instance wraps
        /// </summary>
        public readonly Stream BaseStream;

        private readonly byte[] _buffer = new byte[sizeof(decimal)];
        private readonly Decoder _decoder = Encoding.UTF8.GetDecoder();
        private readonly Encoder _encoder = Encoding.UTF8.GetEncoder();

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
        public T[] ReadArray<T>(int count, int order, bool enableSwap, T[] target = null) where T : struct {
            target ??= new T[count];
            ReadSpan<T>(target, count, order, enableSwap);
            return target;
        }

        /// <summary>
        /// Read list
        /// </summary>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <param name="target">Optional existing array to operate on</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <returns>Array</returns>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public List<T> ReadList<T>(int count, int order, bool enableSwap, List<T> target = null) where T : struct {
            target ??= new List<T>();
            target.AddRange(ReadArray<T>(count, order, enableSwap));
            return target;
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
        public unsafe void ReadSpan<T>(Span<T> target, int count, int order, bool enableSwap) where T : struct {
            var mainTarget = MemoryMarshal.Cast<T, byte>(target);
            var mainLen = count * order;
            var buf = ArrayPool<byte>.Shared.Rent(4096);
            var span = buf.AsSpan();
            try {
                int left = mainLen, read, tot = 0;
                do {
                    read = BaseStream.Read(buf, 0, Math.Min(4096, left));
                    span.Slice(0, read).CopyTo(mainTarget.Slice(tot));
                    left -= read;
                    tot += read;
                } while (left > 0);

                if (left > 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{read:X} read, 0x{left:X} left, 0x{BaseStream.Position:X} end position");

                if (enableSwap && Swap) {
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
                                for (var i = 0; i < mainLen; i += 2) {
                                    var tmp = p[i];
                                    p[i] = p[i + 3];
                                    p[i + 3] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 2];
                                    p[i + 2] = tmp;
                                }

                                break;
                            case 8:
                                for (var i = 0; i < mainLen; i += 2) {
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
                ArrayPool<byte>.Shared.Return(buf);
            }
        }

        /// <summary>
        /// Write list
        /// </summary>
        /// <param name="source">Source list</param>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        public void WriteList<T>(List<T> source, int count, int order, bool enableSwap) where T : struct =>
            WriteSpan<T>(source.ToArray(), count, order, enableSwap);

        /// <summary>
        /// Write span
        /// </summary>
        /// <param name="source">Source buffer</param>
        /// <param name="count">Number of elements</param>
        /// <param name="order">Length of each element</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        public unsafe void WriteSpan<T>(Span<T> source, int count, int order, bool enableSwap) where T : struct {
            var mainTarget = MemoryMarshal.Cast<T, byte>(source);
            var buf = ArrayPool<byte>.Shared.Rent(4096);
            var span = buf.AsSpan();
            var left = count * order;
            var tot = 0;
            try {
                if (!enableSwap || !Swap) {
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
                                for (var i = 0; i < cur; i += 2) {
                                    var tmp = p[i];
                                    p[i] = p[i + 3];
                                    p[i + 3] = tmp;
                                    tmp = p[i + 1];
                                    p[i + 1] = p[i + 2];
                                    p[i + 2] = tmp;
                                }

                                break;
                            case 8:
                                for (var i = 0; i < cur; i += 2) {
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
                ArrayPool<byte>.Shared.Return(buf);
            }
        }

        private void ReadBase(int length) {
            int read, tot = 0;
            do {
                read = BaseStream.Read(_buffer, tot, length);
                length -= read;
                tot += read;
            } while (length > 0 && read != 0);

            if (length > 0)
                throw new ApplicationException(
                    $"Failed to read required number of bytes! 0x{read:X} read, 0x{length:X} left, 0x{BaseStream.Position:X} end position");
        }

        /// <summary>
        /// Read UTF-8 string
        /// </summary>
        /// <returns>Decoded string</returns>
        public unsafe string ReadUtf8String() {
            var b = new StringBuilder();
            var tmpBuf = ArrayPool<byte>.Shared.Rent(4096);
            //var tmpBufOut = ArrayPool<byte>.Shared.Rent(4096);
            try {
                fixed (byte* tmpBufPtr = tmpBuf /*, tmpBufOutPtr = tmpBufOut*/) {
                    var charPtr = (char*) (tmpBufPtr + 2048); //tmpBufOutPtr;
                    int len;
                    while ((len = Read7S32(out _)) != 0) {
                        int read;
                        do {
                            read = BaseStream.Read(tmpBuf, 0, Math.Min(len, /*4096*/ 2048));
                            len -= read;
                            var cur = 0;
                            do {
                                _decoder.Convert(tmpBufPtr + cur, read - cur, charPtr, /*4096*/ 2048 / sizeof(char),
                                    false, out var numIn, out var numOut, out _);
                                b.Append(charPtr, numOut);
                                cur += numIn;
                            } while (cur != read);
                        } while (len > 0 && read != 0);

                        if (len > 0)
                            throw new ApplicationException(
                                $"Failed to read required number of bytes! 0x{read:X} read, 0x{len:X} left, 0x{BaseStream.Position:X} end position");
                    }
                }
            }
            finally {
                ArrayPool<byte>.Shared.Return(tmpBuf);
                //ArrayPool<byte>.Shared.Return(tmpBufOut);
            }

            return b.ToString();
        }

        /// <summary>
        /// Write UTF-8 string
        /// </summary>
        /// <param name="value">String to write</param>
        public unsafe void WriteUtf8String(string value) {
            var tmpBuf = ArrayPool<byte>.Shared.Rent(4096);
            try {
                fixed (char* strPtr = value) {
                    fixed (byte* tmpBufPtr = tmpBuf) {
                        var vStringOfs = 0;
                        var vStringLeft = value.Length;
                        while (vStringLeft > 0) {
                            _encoder.Convert(strPtr + vStringOfs, vStringLeft, tmpBufPtr, 4096, false,
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
                ArrayPool<byte>.Shared.Return(tmpBuf);
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
        public unsafe short ReadS16() {
            ReadBase(sizeof(short));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(short*) buffer;
                var tmp = *buffer;
                *buffer = buffer[1];
                buffer[1] = tmp;

                return *(short*) buffer;
            }
        }

        /// <summary>
        /// Write signed 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteS16(short value) {
            fixed (byte* buffer = _buffer) {
                *(short*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[1];
                    buffer[1] = tmp;
                }
            }

            BaseStream.Write(_buffer, 0, sizeof(short));
        }

        /// <summary>
        /// Read unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe ushort ReadU16() {
            ReadBase(sizeof(ushort));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(ushort*) buffer;
                var tmp = *buffer;
                *buffer = buffer[1];
                buffer[1] = tmp;

                return *(ushort*) buffer;
            }
        }

        /// <summary>
        /// Write unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteU16(ushort value) {
            fixed (byte* buffer = _buffer) {
                *(ushort*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[1];
                    buffer[1] = tmp;
                }
            }

            BaseStream.Write(_buffer, 0, sizeof(ushort));
        }

        /// <summary>
        /// Read signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe int ReadS32() {
            ReadBase(sizeof(int));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(int*) buffer;
                var tmp = *buffer;
                *buffer = buffer[3];
                buffer[3] = tmp;
                tmp = buffer[1];
                buffer[1] = buffer[2];
                buffer[2] = tmp;

                return *(int*) buffer;
            }
        }

        /// <summary>
        /// Write signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteS32(int value) {
            fixed (byte* buffer = _buffer) {
                *(int*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[3];
                    buffer[3] = tmp;
                    tmp = buffer[1];
                    buffer[1] = buffer[2];
                    buffer[2] = tmp;
                }
            }

            BaseStream.Write(_buffer, 0, sizeof(int));
        }

        /// <summary>
        /// Read unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe uint ReadU32() {
            ReadBase(sizeof(uint));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(uint*) buffer;
                var tmp = *buffer;
                *buffer = buffer[3];
                buffer[3] = tmp;
                tmp = buffer[1];
                buffer[1] = buffer[2];
                buffer[2] = tmp;

                return *(uint*) buffer;
            }
        }

        /// <summary>
        /// Write unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteU32(uint value) {
            fixed (byte* buffer = _buffer) {
                *(uint*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[3];
                    buffer[3] = tmp;
                    tmp = buffer[1];
                    buffer[1] = buffer[2];
                    buffer[2] = tmp;
                }
            }

            BaseStream.Write(_buffer, 0, sizeof(uint));
        }

        /// <summary>
        /// Read signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe long ReadS64() {
            ReadBase(sizeof(long));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(long*) buffer;
                var tmp = *buffer;
                *buffer = buffer[7];
                buffer[7] = tmp;
                tmp = buffer[1];
                buffer[1] = buffer[6];
                buffer[6] = tmp;
                tmp = buffer[2];
                buffer[2] = buffer[5];
                buffer[5] = tmp;
                tmp = buffer[3];
                buffer[3] = buffer[4];
                buffer[4] = tmp;

                return *(long*) buffer;
            }
        }

        /// <summary>
        /// Write signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteS64(long value) {
            fixed (byte* buffer = _buffer) {
                *(long*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[7];
                    buffer[7] = tmp;
                    tmp = buffer[1];
                    buffer[1] = buffer[6];
                    buffer[6] = tmp;
                    tmp = buffer[2];
                    buffer[2] = buffer[5];
                    buffer[5] = tmp;
                    tmp = buffer[3];
                    buffer[3] = buffer[4];
                    buffer[4] = tmp;
                }
            }

            BaseStream.Write(_buffer, 0, sizeof(long));
        }

        /// <summary>
        /// Read unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe ulong ReadU64() {
            ReadBase(sizeof(ulong));
            fixed (byte* buffer = _buffer) {
                if (!Swap) return *(ulong*) buffer;
                var tmp = *buffer;
                *buffer = buffer[7];
                buffer[7] = tmp;
                tmp = buffer[1];
                buffer[1] = buffer[6];
                buffer[6] = tmp;
                tmp = buffer[2];
                buffer[2] = buffer[5];
                buffer[5] = tmp;
                tmp = buffer[3];
                buffer[3] = buffer[4];
                buffer[4] = tmp;

                return *(ulong*) buffer;
            }
        }

        /// <summary>
        /// Write unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public unsafe void WriteU64(ulong value) {
            fixed (byte* buffer = _buffer) {
                *(ulong*) buffer = value;
                if (Swap) {
                    var tmp = *buffer;
                    *buffer = buffer[7];
                    buffer[7] = tmp;
                    tmp = buffer[1];
                    buffer[1] = buffer[6];
                    buffer[6] = tmp;
                    tmp = buffer[2];
                    buffer[2] = buffer[5];
                    buffer[5] = tmp;
                    tmp = buffer[3];
                    buffer[3] = buffer[4];
                    buffer[4] = tmp;
                }
            }

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