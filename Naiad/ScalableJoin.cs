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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Naiad
{
    public static class ScalableJoin
    {
        public struct Request<K> : Naiadable<Request<K>>
            where K : IEquatable<K>
        {
            public K key;
            public int index;

            public SubArray<byte> Serialize(SubArray<byte> target) { return target; }      // returns remaining viable region of target
            public SubArray<byte> Deserialize(SubArray<byte> target) { return target; }    // returns remaining valid region of target

            public override int GetHashCode() { return key.GetHashCode() + 1431234 * index; }
            public bool Equals(Request<K> that) { return this.key.Equals(that.key) && this.index == that.index; }

            public Request(K k, int i) { key = k; index = i; }
        }

        public struct Response<K> : Naiadable<Response<K>>
            where K : IEquatable<K>
        {
            public K key;
            public int index1;
            public int index2;

            public SubArray<byte> Serialize(SubArray<byte> target) { return target; }      // returns remaining viable region of target
            public SubArray<byte> Deserialize(SubArray<byte> target) { return target; }    // returns remaining valid region of target

            public override int GetHashCode() { return key.GetHashCode() + 1431234 * index1 + 1341 * index2; }
            public bool Equals(Response<K> that) { return this.key.Equals(that.key) && this.index1 == that.index1 && this.index2 == that.index2; }

            public Response(K k, int i1, int i2) { key = k; index1 = i1; index2 = i2; }
        }

        public struct PartialResult<S1> : Naiadable<PartialResult<S1>>
            where S1 : Naiadable<S1>
        {
            public S1 value1;
            public int index2;

            public SubArray<byte> Serialize(SubArray<byte> target) { return target; }      // returns remaining viable region of target
            public SubArray<byte> Deserialize(SubArray<byte> target) { return target; }    // returns remaining valid region of target

            public bool Equals(PartialResult<S1> that) { return this.value1.Equals(that.value1) && this.index2 == that.index2; }
            //public int CompareTo(PartialResult<S1> that) { if (this.index2 != that.index2) return this.index2 - that.index2; else return this.value1.CompareTo(that.value1); }

            public PartialResult(S1 v, int i) { value1 = v; index2 = i; }
        }

        public static CollectionInterfaceToBeRenamed<R, T> Join<K, S1, S2, V1, V2, T, R>(this CollectionInterfaceToBeRenamed<S1, T> input1,
                                                                          CollectionInterfaceToBeRenamed<S2, T> input2,
                                                                          Func<S1, K> key1,
                                                                          Func<S2, K> key2,
                                                                          Func<K, S1, S2, R> result)
            where K : IEquatable<K>
            where S1 : Naiadable<S1>
            where S2 : Naiadable<S2>
            where T : Lattice<T>
            where R : Naiadable<R>
        {
            var block = 2;

            // this produces the aggregation tree for requests. Should result in a (key, 0) request for each key, along with the rest of the tree.
            var requests1 = input1.Select(input => new Request<K>(key1(input), int.MaxValue & input.GetHashCode()))
                                  .FixedPoint(reqs => reqs.Select(req => new Request<K>(req.key, req.index / block))
                                                          .Concat(reqs)
                                                          .Distinct());

            var requests2 = input2.Select(input => new Request<K>(key2(input), int.MaxValue & input.GetHashCode()))
                                  .FixedPoint(reqs => reqs.Select(req => new Request<K>(req.key, req.index / block))
                                                          .Concat(reqs)
                                                          .Distinct());

            var responses = requests1.Where(req => req.index == 0)
                                     .Intersect(requests2.Where(req => req.index == 0))
                                     .Select(req => new Response<K>(req.key, 0, 0))
                                     .FixedPoint(rs => rs.SelectMany(rsp => Enumerable.Range(0, block * block)
                                                                                      .Select(i => new Response<K>(rsp.key,
                                                                                                                   block * rsp.index1 + (i % block),
                                                                                                                   block * rsp.index2 + (i / block))))
                                                         .Join(requests1.ExtendTime(), rsp => new Request<K>(rsp.key, rsp.index1), req => req, (rsp, req) => rsp)
                                                         .Join(requests2.ExtendTime(), rsp => new Request<K>(rsp.key, rsp.index2), req => req, (rsp, req) => rsp)
                                                         .Concat(rs));

            return responses.Join(input1, rsp => new Request<K>(rsp.key, rsp.index1),
                                          input => new Request<K>(key1(input), int.MaxValue & input.GetHashCode()),
                                          (rsp, input) => new PartialResult<S1>(input, rsp.index2))
                            .Join(input2, rsp => new Request<K>(key1(rsp.value1), rsp.index2),
                                          input => new Request<K>(key2(input), int.MaxValue & input.GetHashCode()),
                                          (rsp, input) => result(key2(input), rsp.value1, input));
        }
    }
}
