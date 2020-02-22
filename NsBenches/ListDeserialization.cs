using System;
using System.Collections.Generic;
using System.IO;
using BenchmarkDotNet.Attributes;
using Ns;

namespace NsBenches {
    public class ListDeserialization {
        private const int N = 100000;
        private const int Ns = 1000;
        private const int Nss = 10;
        private readonly MemoryStream _ms;
        private readonly NetSerializer _ns;

        public ListDeserialization() {
            var data = new byte[N * sizeof(long)];
            new Random(42).NextBytes(data);
            _ms = new MemoryStream(data);
            _ns = new NetSerializer(_ms);
        }

        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        // ReSharper disable once MemberCanBePrivate.Global
        [Params(Nss, Ns, N)] public int C { get; set; }

        [Benchmark]
        public List<byte> NonBufferedByte() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => {
                var res = new List<byte> {Capacity = C};
                for (var i = 0; i < C; i++) res.Add(s.ReadU8());

                return res;
            });
        }

        [Benchmark]
        public List<byte> BufferedByte() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => s.ReadList<byte>(C, true));
        }

        [Benchmark]
        public List<int> NonBufferedInt() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => {
                var res = new List<int> {Capacity = C};
                for (var i = 0; i < C; i++) res.Add(s.ReadS32());

                return res;
            });
        }

        [Benchmark]
        public List<int> BufferedInt() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => s.ReadList<int>(C, true));
        }

        [Benchmark]
        public List<long> NonBufferedLong() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => {
                var res = new List<long> {Capacity = C};
                for (var i = 0; i < C; i++) res.Add(s.ReadS64());

                return res;
            });
        }

        [Benchmark]
        public List<long> BufferedLong() {
            _ms.Position = 0;
            return _ns.Deserialize(false, s => s.ReadList<long>(C, true));
        }
    }
}