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
    ///     Manages serialization to / deserialization from a stream
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "UnusedMethodReturnValue.Global")]
    public class NetSerializer {
        private static readonly bool Swap = !BitConverter.IsLittleEndian;

        private static readonly Dictionary<Type, (object encoder, object decoder)> Converters =
            new Dictionary<Type, (object encoder, object decoder)>();

        private readonly byte[] _buffer = new byte[sizeof(decimal)];
        private Decoder _decoder;
        private Encoder _encoder;
        private StringBuilder _stringBuilder;

        static NetSerializer() {
            #region Register primitives

            #region Base primitives

            RegisterInternal((s, o) => s.WriteU8((byte) (o ? 1 : 0)), s => s.ReadU8() != 0);
            RegisterInternal((s, o) => s.WriteU8(o), s => s.ReadU8());
            RegisterInternal((s, o) => s.WriteS8(o), s => s.ReadS8());
            RegisterInternal((s, o) => s.WriteU16(o), s => s.ReadU16());
            RegisterInternal((s, o) => s.WriteS16(o), s => s.ReadS16());
            RegisterInternal((s, o) => s.WriteU32(o), s => s.ReadU32());
            RegisterInternal((s, o) => s.WriteS32(o), s => s.ReadS32());
            RegisterInternal((s, o) => s.WriteU64(o), s => s.ReadU64());
            RegisterInternal((s, o) => s.WriteS64(o), s => s.ReadS64());
            RegisterInternal((s, o) => s.WriteSingle(o), s => s.ReadSingle());
            RegisterInternal((s, o) => s.WriteDouble(o), s => s.ReadDouble());
            RegisterInternal((s, o) => s.WriteDecimal(o), s => s.ReadDecimal());
            RegisterInternal((s, o) => s.WriteU16(o), s => (char) s.ReadU16());
            RegisterInternal((s, o) => s.WriteGuid(o), s => s.ReadGuid());

            #endregion

            #region Arrays

            RegisterInternal(MakeNativeArrayEncoder<bool>(false), MakeNativeArrayDecoder<bool>(false));
            RegisterInternal(MakeNativeArrayEncoder<byte>(false), MakeNativeArrayDecoder<byte>(false));
            RegisterInternal(MakeNativeArrayEncoder<sbyte>(false), MakeNativeArrayDecoder<sbyte>(false));
            RegisterInternal(MakeNativeArrayEncoder<ushort>(true), MakeNativeArrayDecoder<ushort>(true));
            RegisterInternal(MakeNativeArrayEncoder<short>(true), MakeNativeArrayDecoder<short>(true));
            RegisterInternal(MakeNativeArrayEncoder<uint>(true), MakeNativeArrayDecoder<uint>(true));
            RegisterInternal(MakeNativeArrayEncoder<int>(true), MakeNativeArrayDecoder<int>(true));
            RegisterInternal(MakeNativeArrayEncoder<ulong>(true), MakeNativeArrayDecoder<ulong>(true));
            RegisterInternal(MakeNativeArrayEncoder<long>(true), MakeNativeArrayDecoder<long>(true));
            RegisterInternal(MakeNativeArrayEncoder<float>(false), MakeNativeArrayDecoder<float>(false));
            RegisterInternal(MakeNativeArrayEncoder<double>(false), MakeNativeArrayDecoder<double>(false));
            RegisterInternal(MakeNativeArrayEncoder<char>(true), MakeNativeArrayDecoder<char>(true));
            RegisterInternal(MakeNativeArrayEncoder<decimal>(false), MakeNativeArrayDecoder<decimal>(false));
            RegisterInternal(MakeNativeArrayEncoder<Guid>(false), MakeNativeArrayDecoder<Guid>(false));

            #endregion

            #region Lists

            RegisterInternal((s, bList) => {
                    foreach (var b in bList) s.WriteU8((byte) (b ? 1 : 0));
                },
                s => s.ReadList<bool>(s.ReadS32(), false));

            RegisterInternal((s, u8List) => {
                    s.WriteS32(u8List.Count);
                    foreach (var u8 in u8List) s.WriteU8(u8);
                },
                s => s.ReadList<byte>(s.ReadS32(), false));

            RegisterInternal((s, s8List) => {
                    s.WriteS32(s8List.Count);
                    foreach (var s8 in s8List) s.WriteS8(s8);
                },
                s => s.ReadList<sbyte>(s.ReadS32(), false));

            RegisterInternal((s, u16List) => {
                    s.WriteS32(u16List.Count);
                    foreach (var u16 in u16List) s.WriteU16(u16);
                },
                s => s.ReadList<ushort>(s.ReadS32(), true));

            RegisterInternal((s, s16List) => {
                    s.WriteS32(s16List.Count);
                    foreach (var s16 in s16List) s.WriteS16(s16);
                },
                s => s.ReadList<short>(s.ReadS32(), true));

            RegisterInternal((s, u32List) => {
                    s.WriteS32(u32List.Count);
                    foreach (var u32 in u32List) s.WriteU32(u32);
                },
                s => s.ReadList<uint>(s.ReadS32(), true));

            RegisterInternal((s, s32List) => {
                    s.WriteS32(s32List.Count);
                    foreach (var s32 in s32List) s.WriteS32(s32);
                },
                s => s.ReadList<int>(s.ReadS32(), true));

            RegisterInternal((s, u64List) => {
                    s.WriteS32(u64List.Count);
                    foreach (var u64 in u64List) s.WriteU64(u64);
                },
                s => s.ReadList<ulong>(s.ReadS32(), true));

            RegisterInternal((s, s64List) => {
                    s.WriteS32(s64List.Count);
                    foreach (var s64 in s64List) s.WriteS64(s64);
                },
                s => s.ReadList<long>(s.ReadS32(), true));

            RegisterInternal((s, fList) => {
                    s.WriteS32(fList.Count);
                    foreach (var f in fList) s.WriteSingle(f);
                },
                s => s.ReadList<float>(s.ReadS32(), false));

            RegisterInternal((s, dList) => {
                    s.WriteS32(dList.Count);
                    foreach (var d in dList) s.WriteDouble(d);
                },
                s => s.ReadList<double>(s.ReadS32(), false));

            RegisterInternal((s, deList) => {
                    s.WriteS32(deList.Count);
                    foreach (var de in deList) s.WriteDecimal(de);
                },
                s => s.ReadList<decimal>(s.ReadS32(), false));

            RegisterInternal((s, cList) => {
                    s.WriteS32(cList.Count);
                    foreach (var c in cList) s.WriteU16(c);
                },
                s => s.ReadList<char>(s.ReadS32(), false));

            RegisterInternal((s, guList) => {
                    s.WriteS32(guList.Count);
                    foreach (var gu in guList) s.WriteGuid(gu);
                },
                s => s.ReadList<Guid>(s.ReadS32(), false));

            #endregion

            #endregion

            #region Register strings

            RegisterInternal((s, o) => s.WriteUtf8String(o),
                s => s.ReadUtf8String());

            RegisterInternal((s, strArr) => {
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

            RegisterInternal((s, strList) => {
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

            AddDictionary<string, string>();
            AddDictionary<string, byte>();
            AddDictionary<string, sbyte>();
            AddDictionary<string, ushort>();
            AddDictionary<string, short>();
            AddDictionary<string, uint>();
            AddDictionary<string, int>();
            AddDictionary<string, ulong>();
            AddDictionary<string, long>();
            AddDictionary<string, float>();
            AddDictionary<string, double>();
            AddDictionary<string, decimal>();
            AddDictionary<string, char>();
            AddDictionary<string, Guid>();

            #endregion
        }

        /// <summary>
        ///     Create new NetSerializer instance
        /// </summary>
        /// <param name="baseStream">Stream to wrap</param>
        /// <param name="converters">Custom converters to use</param>
        public NetSerializer(Stream baseStream,
            IReadOnlyDictionary<Type, (object encoder, object decoder)> converters = null) {
            BaseStream = baseStream;
            CustomConverters = converters;
        }

        /// <summary>
        ///     Stream this instance wraps
        /// </summary>
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public Stream BaseStream { get; set; }

        /// <summary>
        ///     Converters specific to this serializer
        /// </summary>
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
        public IReadOnlyDictionary<Type, (object encoder, object decoder)> CustomConverters { get; set; }

        private Decoder Decoder => _decoder ??= Encoding.UTF8.GetDecoder();
        private Encoder Encoder => _encoder ??= Encoding.UTF8.GetEncoder();
        private StringBuilder StringBuilder => _stringBuilder ??= new StringBuilder();

        private static void RegisterInternal<T>(Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder) {
            Converters[typeof(T)] = (encoder, decoder);
        }

        private static Action<NetSerializer, T[]> MakeNativeArrayEncoder<T>(bool enableSwap)
            where T : unmanaged {
            return (s, arr) => {
                {
                    s.WriteS32(arr.Length);
                    s.WriteSpan<T>(arr, arr.Length, enableSwap);
                }
            };
        }

        private static Func<NetSerializer, T[]> MakeNativeArrayDecoder<T>(bool enableSwap)
            where T : unmanaged {
            return s => s.ReadArray<T>(s.ReadS32(), enableSwap);
        }

        private static Action<NetSerializer, T[]> MakeArrayEncoder<T>(Action<NetSerializer, T> encoder, bool nc) {
            return (s, arr) => {
                s.WriteS32(arr.Length);
                foreach (var e in arr) s.Serialize(e, nc, encoder);
            };
        }

        private static Func<NetSerializer, T[]> MakeArrayDecoder<T>(Func<NetSerializer, T> decoder, bool nc) {
            return s => {
                var count = s.ReadS32();
                var res = new T[count];
                for (var i = 0; i < count; i++) res[i] = s.Deserialize(nc, decoder);
                return res;
            };
        }

        private static Action<NetSerializer, List<T>> MakeListEncoder<T>(Action<NetSerializer, T> encoder, bool nc) {
            return (s, list) => {
                s.WriteS32(list.Count);
                foreach (var e in list) s.Serialize(e, nc, encoder);
            };
        }

        private static Func<NetSerializer, List<T>> MakeListDecoder<T>(Func<NetSerializer, T> decoder, bool nc) {
            return s => {
                var count = s.ReadS32();
                var res = new List<T>(count);
                for (var i = 0; i < count; i++) res.Add(s.Deserialize(nc, decoder));
                return res;
            };
        }

        private static Action<NetSerializer, Dictionary<TKey, TValue>> MakeDictionaryEncoder<TKey, TValue>(
            Action<NetSerializer, TKey> keyEncoder, bool keyNc, Action<NetSerializer, TValue> valueEncoder,
            bool valueNc) {
            return (s, dict) => {
                s.WriteS32(dict.Count);
                foreach (var kvp in dict) {
                    s.Serialize(kvp.Key, keyNc, keyEncoder);
                    s.Serialize(kvp.Value, valueNc, valueEncoder);
                }
            };
        }

        private static Func<NetSerializer, Dictionary<TKey, TValue>> MakeDictionaryDecoder<TKey, TValue>(
            Func<NetSerializer, TKey> keyDecoder, bool keyNc, Func<NetSerializer, TValue> valueDecoder, bool valueNc) {
            return s => {
                var dict = new Dictionary<TKey, TValue>();
                var count = s.ReadS32();
                for (var i = 0; i < count; i++) {
                    var k = s.Deserialize(keyNc, keyDecoder);
                    var v = s.Deserialize(valueNc, valueDecoder);
                    if (k != null) dict[k] = v;
                }

                return dict;
            };
        }

        private static void StrEncoder(NetSerializer s, string str) {
            s.WriteUtf8String(str);
        }

        private static string StrDecoder(NetSerializer s) {
            return s.ReadUtf8String();
        }

        /// <summary>
        ///     Register a type's encoder and decoder
        /// </summary>
        /// <param name="encoder">Encoder for object to stream</param>
        /// <param name="decoder">Decoder for stream to object</param>
        /// <param name="generateCollectionConverters">Generate converters for basic collections</param>
        public static void Register<T>(Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder,
            bool generateCollectionConverters = true) {
            RegisterInternal(encoder, decoder);
            if (!generateCollectionConverters) return;
            var nc = !typeof(T).IsValueType;
            RegisterInternal(MakeArrayEncoder(encoder, nc), MakeArrayDecoder(decoder, nc));
            RegisterInternal(MakeListEncoder(encoder, nc), MakeListDecoder(decoder, nc));
            RegisterInternal(MakeDictionaryEncoder<string, T>(StrEncoder, true, encoder, nc),
                MakeDictionaryDecoder(StrDecoder, true, decoder, nc));
        }

        /// <summary>
        ///     Register a type's encoder and decoder
        /// </summary>
        /// <param name="converters">Custom converter mapping</param>
        /// <param name="encoder">Encoder for object to stream</param>
        /// <param name="decoder">Decoder for stream to object</param>
        /// <param name="generateCollectionConverters">Generate converters for basic collections</param>
        public static void RegisterCustom<T>(Dictionary<Type, (object encoder, object decoder)> converters,
            Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder,
            bool generateCollectionConverters = true) {
            converters[typeof(T)] = (encoder, decoder);
            if (!generateCollectionConverters) return;
            var nc = !typeof(T).IsValueType;
            converters[typeof(T[])] = (MakeArrayEncoder(encoder, nc), MakeArrayDecoder(decoder, nc));
            converters[typeof(List<T>)] = (MakeListEncoder(encoder, nc), MakeListDecoder(decoder, nc));
            converters[typeof(Dictionary<string, T>)] = (
                MakeDictionaryEncoder<string, T>(StrEncoder, true, encoder, nc),
                MakeDictionaryDecoder(StrDecoder, true, decoder, nc));
        }

        /// <summary>
        ///     Register dictionary type
        /// </summary>
        /// <typeparam name="TKey">Dictionary key type</typeparam>
        /// <typeparam name="TValue">Dictionary value type</typeparam>
        /// <exception cref="ApplicationException">If unregistered types are used</exception>
        public static void AddDictionary<TKey, TValue>() {
            if (!Converters.TryGetValue(typeof(TKey), out var resKey))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary key type is not already registered");
            if (!Converters.TryGetValue(typeof(TValue), out var resValue))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary value type is not already registered");
            var (keyE, keyD) = ((Action<NetSerializer, TKey>) resKey.encoder,
                (Func<NetSerializer, TKey>) resKey.decoder);
            var (valueE, valueD) = ((Action<NetSerializer, TValue>) resValue.encoder,
                (Func<NetSerializer, TValue>) resValue.decoder);
            var ncKey = !typeof(TKey).IsValueType;
            var ncValue = !typeof(TValue).IsValueType;
            RegisterInternal(MakeDictionaryEncoder(keyE, ncKey, valueE, ncValue),
                MakeDictionaryDecoder(keyD, ncKey, valueD, ncValue));
        }

        /// <summary>
        ///     Register dictionary type
        /// </summary>
        /// <param name="converters">Custom converter mapping</param>
        /// <typeparam name="TKey">Dictionary key type</typeparam>
        /// <typeparam name="TValue">Dictionary value type</typeparam>
        /// <exception cref="ApplicationException">If unregistered types are used</exception>
        public static void AddDictionaryCustom<TKey, TValue>(
            Dictionary<Type, (object encoder, object decoder)> converters) {
            if (!GetConverterCustom<TKey>(converters, out var key, true))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary key type is not already registered");
            if (!GetConverterCustom<TValue>(converters, out var value, true))
                throw new ApplicationException(
                    "Cannot add dictionary converters if dictionary value type is not already registered");
            var (keyE, keyD) = key;
            var (valueE, valueD) = value;
            var ncKey = !typeof(TKey).IsValueType;
            var ncValue = !typeof(TValue).IsValueType;
            converters[typeof(Dictionary<TKey, TValue>)] = (MakeDictionaryEncoder(keyE, ncKey, valueE, ncValue),
                MakeDictionaryDecoder(keyD, ncKey, valueD, ncValue));
        }

        /// <summary>
        ///     Get encoder/decoder pair for type
        /// </summary>
        /// <param name="res">Encoder and decoder</param>
        /// <typeparam name="T">Type to retrieve encoder/decoder for</typeparam>
        /// <returns>True if encoder/decoder were obtained</returns>
        public static bool GetConverter<T>(
            out (Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder) res) {
            if (Converters.TryGetValue(typeof(T), out var res1)) {
                res = ((Action<NetSerializer, T>) res1.encoder, (Func<NetSerializer, T>) res1.decoder);
                return true;
            }

            res = default;
            return false;
        }

        /// <summary>
        ///     Get encoder/decoder pair for type
        /// </summary>
        /// <param name="converters">Custom converter mapping</param>
        /// <param name="res">Encoder and decoder</param>
        /// <param name="withGlobal">Enable global converters</param>
        /// <typeparam name="T">Type to retrieve encoder/decoder for</typeparam>
        /// <returns>True if encoder/decoder were obtained</returns>
        public static bool GetConverterCustom<T>(IReadOnlyDictionary<Type, (object encoder, object decoder)> converters,
            out (Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder) res, bool withGlobal = false) {
            res = default;
            if (!converters.TryGetValue(typeof(T), out var resX))
                return withGlobal && GetConverter(out res);
            if (!(resX.encoder is Action<NetSerializer, T> encoder)) return false;
            if (!(resX.decoder is Func<NetSerializer, T> decoder)) return false;
            res = (encoder, decoder);
            return true;
        }

        /// <summary>
        ///     Get encoder/decoder pair for type
        /// </summary>
        /// <param name="res">Encoder and decoder</param>
        /// <typeparam name="T">Type to retrieve encoder/decoder for</typeparam>
        /// <returns>True if encoder/decoder were obtained</returns>
        public bool GetConverterLocal<T>(
            out (Action<NetSerializer, T> encoder, Func<NetSerializer, T> decoder) res) {
            return CustomConverters != null && GetConverterCustom(CustomConverters, out res) || GetConverter(out res);
        }

        /// <summary>
        ///     Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj) {
            if (!GetConverterLocal<T>(out var res))
                throw new ApplicationException("Tried to serialize unregistered type");
            Serialize(obj, !typeof(T).IsValueType, res.encoder);
        }

        /// <summary>
        ///     Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <param name="nullCheck">Serialize null state</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj, bool nullCheck) {
            if (!GetConverterLocal<T>(out var res))
                throw new ApplicationException("Tried to serialize unregistered type");
            Serialize(obj, nullCheck, res.encoder);
        }

        /// <summary>
        ///     Serialize an object
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <param name="nullCheck">Serialize null state</param>
        /// <param name="encoder">Encoder to use</param>
        /// <typeparam name="T">Type of object to be serialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        public void Serialize<T>(T obj, bool nullCheck, Action<NetSerializer, T> encoder) {
            if (nullCheck) {
                WriteU8((byte) (obj != null ? 1 : 0));
                if (obj == null) return;
            }

            encoder.Invoke(this, obj);
        }

        /// <summary>
        ///     Deserialize an object
        /// </summary>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <exception cref="ApplicationException">If attempting to serialize unregistered type</exception>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>() {
            if (!GetConverterLocal<T>(out var res))
                throw new ApplicationException("Tried to deserialize unregistered type");
            return Deserialize(!typeof(T).IsValueType, res.decoder);
        }

        /// <summary>
        ///     Deserialize an object
        /// </summary>
        /// <param name="nullCheck">Deserialize null state</param>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>(bool nullCheck) {
            if (!GetConverterLocal<T>(out var res))
                throw new ApplicationException("Tried to deserialize unregistered type");
            return Deserialize(nullCheck, res.decoder);
        }

        /// <summary>
        ///     Deserialize an object
        /// </summary>
        /// <param name="nullCheck">Deserialize null state</param>
        /// <param name="decoder">Decoder to use</param>
        /// <typeparam name="T">Type of object to be deserialized</typeparam>
        /// <returns>Deserialized object or null</returns>
        public T Deserialize<T>(bool nullCheck, Func<NetSerializer, T> decoder) {
            if (nullCheck && ReadU8() == 0) return default;
            return decoder.Invoke(this);
        }

        /// <summary>
        ///     Read array
        /// </summary>
        /// <param name="count">Number of elements</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <param name="target">Optional existing array to operate on</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <returns>Array</returns>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public T[] ReadArray<T>(int count, bool enableSwap, T[] target = null) where T : unmanaged {
            target ??= new T[count];
            ReadSpan<T>(target, count, enableSwap);
            return target;
        }

        /// <summary>
        ///     Read list
        /// </summary>
        /// <param name="count">Number of elements</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <param name="target">Optional existing array to operate on</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <returns>Array</returns>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public unsafe List<T> ReadList<T>(int count, bool enableSwap, List<T> target = null) where T : unmanaged {
            target ??= new List<T>();
            if (count == 0)
                return target;
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
                            if (read == 0)
                                throw new ApplicationException(
                                    $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");
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
                                $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");
                        return target;
                    }

                    do {
                        read = BaseStream.Read(buf, curTot, Math.Min(4096 - curTot, left));
                        if (read == 0)
                            throw new ApplicationException(
                                $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");
                        curTot += read;
                        var trunc = curTot - curTot % order;
                        switch (order) {
                            case 2:
                                var tmp2 = (short*) p;
                                for (var i = 0; i < trunc; i += 2) {
                                    *tmp2 = ReverseEndianness(*tmp2);
                                    target.Add(*(T*) tmp2);
                                    tmp2++;
                                }

                                break;
                            case 4:
                                var tmp4 = (int*) p;
                                for (var i = 0; i < trunc; i += 4) {
                                    *tmp4 = ReverseEndianness(*tmp4);
                                    target.Add(*(T*) tmp4);
                                    tmp4++;
                                }

                                break;
                            case 8:
                                var tmp8 = (long*) p;
                                for (var i = 0; i < trunc; i += 8) {
                                    *tmp8 = ReverseEndianness(*tmp8);
                                    target.Add(*(T*) tmp8);
                                    tmp8++;
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < trunc; i += order) {
                                    for (var j = 0; j < half; j++) {
                                        var fir = i + j;
                                        var sec = i + order - 1 - j;
                                        var tmp = p[fir];
                                        p[fir] = p[sec];
                                        p[sec] = tmp;
                                    }

                                    target.Add(*(T*) (p + i));
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
                            $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");
                    return target;
                }
            }
            finally {
                Shared.Return(buf);
            }
        }

        /// <summary>
        ///     Read span
        /// </summary>
        /// <param name="target">Target buffer</param>
        /// <param name="count">Number of elements</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        /// <exception cref="ApplicationException">If failed to read required number of bytes</exception>
        public unsafe void ReadSpan<T>(Span<T> target, int count, bool enableSwap) where T : unmanaged {
            if (count == 0) return;
            var mainTarget = MemoryMarshal.Cast<T, byte>(target);
            var order = sizeof(T);
            var mainLen = count * order;
            var buf = Shared.Rent(4096);
            var span = buf.AsSpan();
            try {
                int left = mainLen, tot = 0;
                do {
                    var read = BaseStream.Read(buf, 0, Math.Min(4096, left));
                    if (read == 0)
                        throw new ApplicationException(
                            $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");
                    span.Slice(0, read).CopyTo(mainTarget.Slice(tot));
                    left -= read;
                    tot += read;
                } while (left > 0);

                if (left > 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{left:X} left");

                if (order != 1 && enableSwap && Swap)
                    fixed (byte* p = &mainTarget.GetPinnableReference()) {
                        switch (order) {
                            case 2:
                                var tmp2 = (short*) p;
                                for (var i = 0; i < mainLen; i += 2) {
                                    *tmp2 = ReverseEndianness(*tmp2);
                                    tmp2++;
                                }

                                break;
                            case 4:
                                var tmp4 = (int*) p;
                                for (var i = 0; i < mainLen; i += 4) {
                                    *tmp4 = ReverseEndianness(*tmp4);
                                    tmp4++;
                                }

                                break;
                            case 8:
                                var tmp8 = (long*) p;
                                for (var i = 0; i < mainLen; i += 8) {
                                    *tmp8 = ReverseEndianness(*tmp8);
                                    tmp8++;
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < mainLen; i += order)
                                for (var j = 0; j < half; j++) {
                                    var fir = i + j;
                                    var sec = i + order - 1 - j;
                                    var tmp = p[fir];
                                    p[fir] = p[sec];
                                    p[sec] = tmp;
                                }

                                break;
                        }
                    }
            }
            finally {
                Shared.Return(buf);
            }
        }

        /// <summary>
        ///     Write span
        /// </summary>
        /// <param name="source">Source buffer</param>
        /// <param name="count">Number of elements</param>
        /// <param name="enableSwap">Enable element endianness swapping</param>
        /// <typeparam name="T">Type of elements</typeparam>
        public unsafe void WriteSpan<T>(Span<T> source, int count, bool enableSwap) where T : unmanaged {
            if (count == 0)
                return;
            var mainTarget = MemoryMarshal.Cast<T, byte>(source);
            var buf = Shared.Rent(4096);
            var span = buf.AsSpan();
            var order = sizeof(T);
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
                                var tmp2 = (short*) p;
                                for (var i = 0; i < cur; i += 2) {
                                    *tmp2 = ReverseEndianness(*tmp2);
                                    tmp2++;
                                }

                                break;
                            case 4:
                                var tmp4 = (int*) p;
                                for (var i = 0; i < cur; i += 4) {
                                    *tmp4 = ReverseEndianness(*tmp4);
                                    tmp4++;
                                }

                                break;
                            case 8:
                                var tmp8 = (long*) p;
                                for (var i = 0; i < cur; i += 8) {
                                    *tmp8 = ReverseEndianness(*tmp8);
                                    tmp8++;
                                }

                                break;
                            default:
                                var half = order / 2;
                                for (var i = 0; i < cur; i += order)
                                for (var j = 0; j < half; j++) {
                                    var fir = i + j;
                                    var sec = i + order - 1 - j;
                                    var tmp = p[fir];
                                    p[fir] = p[sec];
                                    p[sec] = tmp;
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
        private Span<byte> ReadBase16() {
            var tot = 0;
            do {
                var read = BaseStream.Read(_buffer, tot, sizeof(ushort) - tot);
                if (read == 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{sizeof(ushort) - tot:X} left");
                tot += read;
            } while (tot < sizeof(ushort));

            return _buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> ReadBase32() {
            var tot = 0;
            do {
                var read = BaseStream.Read(_buffer, tot, sizeof(uint) - tot);
                if (read == 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{sizeof(uint) - tot:X} left");
                tot += read;
            } while (tot < sizeof(uint));

            return _buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> ReadBase64() {
            var tot = 0;
            do {
                var read = BaseStream.Read(_buffer, tot, sizeof(ulong) - tot);
                if (read == 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{sizeof(ulong) - tot:X} left");
                tot += read;
            } while (tot < sizeof(ulong));

            return _buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> ReadBase128() {
            var tot = 0;
            do {
                var read = BaseStream.Read(_buffer, tot, sizeof(decimal) - tot);
                if (read == 0)
                    throw new ApplicationException(
                        $"Failed to read required number of bytes! 0x{tot:X} read, 0x{sizeof(decimal) - tot:X} left");
                tot += read;
            } while (tot < sizeof(decimal));

            return _buffer;
        }

        /// <summary>
        ///     Read UTF-8 string
        /// </summary>
        /// <returns>Decoded string</returns>
        public unsafe string ReadUtf8String() {
            Decoder.Reset();
            var tmpBuf = Shared.Rent(4096);
            try {
                fixed (byte* tmpBufPtr = tmpBuf) {
                    var charPtr = (char*) (tmpBufPtr + 2048);
                    int len;
                    while ((len = Read7S32(out _)) != 0) {
                        int tot = 0;
                        do {
                            var read = BaseStream.Read(tmpBuf, 0, Math.Min(len, 2048));
                            if (read == 0)
                                throw new ApplicationException(
                                    $"Failed to read required number of bytes! 0x{tot:X} read, 0x{len:X} left");
                            len -= read;
                            var cur = 0;
                            do {
                                Decoder.Convert(tmpBufPtr + cur, read - cur, charPtr, 2048 / sizeof(char),
                                    false, out var numIn, out var numOut, out _);
                                StringBuilder.Append(charPtr, numOut);
                                cur += numIn;
                            } while (cur != read);

                            tot += read;
                        } while (len > 0);
                    }
                }

                var str = StringBuilder.ToString();
                StringBuilder.Clear();
                return str;
            }
            finally {
                Shared.Return(tmpBuf);
            }
        }

        /// <summary>
        ///     Write UTF-8 string
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
        ///     Decode 32-bit integer
        /// </summary>
        /// <param name="len">Length of decoded value</param>
        /// <returns>Decoded value</returns>
        /// <exception cref="EndOfStreamException">When end of stream was prematurely reached</exception>
        public int Read7S32(out int len) {
            len = 1;
            var bits = 6;
            var c = BaseStream.Read(_buffer, 0, 1);
            if (c == 0)
                throw new EndOfStreamException();
            var v = _buffer[0];
            var value = v & 0x3f;
            var flag = (v & 0x40) != 0;
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
        ///     Encode 32-bit integer
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
        ///     Decode 64-bit integer
        /// </summary>
        /// <param name="len">Length of decoded value</param>
        /// <returns>Decoded value</returns>
        /// <exception cref="EndOfStreamException">When end of stream was prematurely reached</exception>
        public long Read7S64(out int len) {
            len = 1;
            var bits = 6;
            var c = BaseStream.Read(_buffer, 0, 1);
            if (c == 0)
                throw new EndOfStreamException();
            var v = _buffer[0];
            long value = v & 0x3f;
            var flag = (v & 0x40) != 0;
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
        ///     Encode 64-bit integer
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
        ///     Read signed 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public sbyte ReadS8() =>
            BaseStream.Read(_buffer, 0, 1) == 0
                ? throw new ApplicationException(
                    "Failed to read required number of bytes! 0x0 read, 0x1 left")
                : (sbyte) _buffer[0];

        /// <summary>
        ///     Write signed 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS8(sbyte value) {
            _buffer[0] = (byte) value;
            BaseStream.Write(_buffer, 0, sizeof(byte));
        }

        /// <summary>
        ///     Read unsigned 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public byte ReadU8() =>
            BaseStream.Read(_buffer, 0, 1) == 0
                ? throw new ApplicationException(
                    "Failed to read required number of bytes! 0x0 read, 0x1 left")
                : _buffer[0];

        /// <summary>
        ///     Write unsigned 8-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU8(byte value) {
            _buffer[0] = value;
            BaseStream.Write(_buffer, 0, sizeof(byte));
        }

        /// <summary>
        ///     Read signed 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public short ReadS16() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<short>(ReadBase16()))
                : MemoryMarshal.Read<short>(ReadBase16());
        }

        /// <summary>
        ///     Write signed 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS16(short value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(short));
        }

        /// <summary>
        ///     Read unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public ushort ReadU16() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<ushort>(ReadBase16()))
                : MemoryMarshal.Read<ushort>(ReadBase16());
        }

        /// <summary>
        ///     Write unsigned 16-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU16(ushort value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(ushort));
        }

        /// <summary>
        ///     Read signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public int ReadS32() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<int>(ReadBase32()))
                : MemoryMarshal.Read<int>(ReadBase32());
        }

        /// <summary>
        ///     Write signed 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS32(int value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(int));
        }

        /// <summary>
        ///     Read unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public uint ReadU32() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<uint>(ReadBase32()))
                : MemoryMarshal.Read<uint>(ReadBase32());
        }

        /// <summary>
        ///     Write unsigned 32-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU32(uint value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(uint));
        }

        /// <summary>
        ///     Read signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public long ReadS64() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<long>(ReadBase64()))
                : MemoryMarshal.Read<long>(ReadBase64());
        }

        /// <summary>
        ///     Write signed 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteS64(long value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(long));
        }

        /// <summary>
        ///     Read unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public ulong ReadU64() {
            return Swap
                ? ReverseEndianness(
                    MemoryMarshal.Read<ulong>(ReadBase64()))
                : MemoryMarshal.Read<ulong>(ReadBase64());
        }

        /// <summary>
        ///     Write unsigned 64-byte value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteU64(ulong value) {
            if (Swap)
                value = ReverseEndianness(value);
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(ulong));
        }

        /// <summary>
        ///     Read single-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public float ReadSingle() {
            return MemoryMarshal.Read<float>(ReadBase32());
        }

        /// <summary>
        ///     Write single-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteSingle(float value) {
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(float));
        }

        /// <summary>
        ///     Read double-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public double ReadDouble() {
            return MemoryMarshal.Read<double>(ReadBase32());
        }

        /// <summary>
        ///     Write double-precision floating-point value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteDouble(double value) {
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(double));
        }

        /// <summary>
        ///     Read decimal value
        /// </summary>
        /// <returns>Value</returns>
        public decimal ReadDecimal() {
            return MemoryMarshal.Read<decimal>(ReadBase128());
        }

        /// <summary>
        ///     Write decimal value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteDecimal(decimal value) {
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, sizeof(decimal));
        }

        /// <summary>
        ///     Read Guid value
        /// </summary>
        /// <returns>Value</returns>
        public Guid ReadGuid() {
            return MemoryMarshal.Read<Guid>(ReadBase128());
        }

        /// <summary>
        ///     Write Guid value
        /// </summary>
        /// <returns>Value</returns>
        public void WriteGuid(Guid value) {
            MemoryMarshal.Write(_buffer, ref value);
            BaseStream.Write(_buffer, 0, 16);
        }
    }
}