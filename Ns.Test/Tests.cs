using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using NUnit.Framework;

namespace Ns.Test {
    public class Tests {
        [SetUp]
        public void Setup() {
        }

        [Test]
        public void TestBasicSequence() {
            const long l = 100L;
            const char c = 'x';
            var arr = new[] {0, 1, 2, 3, 4, 5};
            var list = new List<int>();
            list.AddRange(arr);
            var listBBuf = new int[4096 + 129];
            new Random().NextBytes(MemoryMarshal.Cast<int, byte>(listBBuf));
            var listB = new List<int>();
            listB.AddRange(listBBuf);
            const string str = "VS.ネビュラグレイ";
            var b = new StringBuilder();
            while (b.Length < 4096 * 4)
                b.Append(str);
            var strLong = b.ToString();
            Assert.IsTrue(strLong.Length >= 4096 * 4);
            const decimal num = 1.05m;
            var dict = new Dictionary<string, string> {
                {"halo", "reach"},
                {"tron", "evolution"},
                {"portal", "2"},
                {"xbox", "360"}
            };
            var strArr = new[] {
                "sounds",
                "like",
                "there's",
                "a",
                "lot",
                "of",
                "hoopla"
            };
            var ms = new MemoryStream();
            var ns = new NetSerializer(ms);
            ns.Serialize(l);
            ns.Serialize(c);
            ns.Serialize(arr);
            ns.Serialize(list);
            ns.Serialize(listB);
            ns.Serialize(str);
            ns.Serialize(num);
            ns.Serialize(dict);
            ns.Serialize(strArr);
            ns.Serialize(strLong);
            ms.Position = 0;
            var l2 = ns.Deserialize<long>();
            var c2 = ns.Deserialize<char>();
            var arr2 = ns.Deserialize<int[]>();
            var list2 = ns.Deserialize(true, s => {
                var count = s.ReadS32();
                var res = new List<int> {Capacity = count};
                for (var i = 0; i < count; i++) res.Add(s.ReadS32());

                return res;
            });
            var listB2 = ns.Deserialize(true, s => s.ReadList<int>(s.ReadS32(), true));
            var str2 = ns.Deserialize<string>();
            var num2 = ns.Deserialize<decimal>();
            var dict2 = ns.Deserialize<Dictionary<string, string>>();
            var strArr2 = ns.Deserialize<string[]>();
            var strLong2 = ns.Deserialize<string>();
            Assert.AreEqual(l, l2);
            Assert.AreEqual(c, c2);
            Assert.AreEqual(arr, arr2);
            Assert.AreEqual(list, list2);
            Assert.AreEqual(listB, listB2);
            Assert.AreEqual(str, str2);
            Assert.AreEqual(num, num2);
            CollectionAssert.AreEquivalent(dict, dict2);
            Assert.AreEqual(strArr, strArr2);
            Assert.AreEqual(strLong, strLong2);
        }
    }
}