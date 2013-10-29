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
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Naiad;
using Naiad.Frameworks.DifferentialDataflow;

namespace Examples.DifferentialDataflow
{
    /// <summary>
    /// Demonstrates a more complicated interactive Naiad program.
    /// This code example was suggested by Russell Power, while an intern at MSR SVC.
    /// </summary>
    public class SearchIndex : Example
    {
        #region Custom datatypes to make things prettier

        public struct Document : IEquatable<Document>
        {
            public string text;
            public int id;

            public bool Equals(Document that)
            {
                return this.text == that.text && this.id == that.id;
            }

            public override int GetHashCode()
            {
                return text.GetHashCode() + id;
            }

            public Document(string t, int i) { text = t; id = i; }
        }

        public struct Query : IEquatable<Query>
        {
            public string text;
            public int id;
            public int threshold;

            public bool Equals(Query that)
            {
                return this.text == that.text && this.id == that.id && this.threshold == that.threshold;
            }

            public override int GetHashCode()
            {
                return text.GetHashCode() + id + threshold;
            }

            public Query(string t, int i, int thr) { text = t; id = i; threshold = thr; }
        }

        public struct Match : IEquatable<Match>
        {
            public int document;
            public int query;
            public int threshold;

            public bool Equals(Match that)
            {
                return this.document == that.document && this.query == that.query && this.threshold == that.threshold;
            }

            public override int GetHashCode()
            {
                return document + query + threshold;
            }

            public Match(int d, int q, int t) { document = d; query = q; threshold = t; }
        }

        #endregion

        public void Execute(string[] args)
        {
            int documentCount = 100000;
            int vocabulary = 100000;
            int batchSize = 10000;
            int iterations = 10;

            using (var controller = NewController.FromArgs(ref args))
            {
                #region building up input data
                
                if (args.Length == 5)
                {
                    documentCount = Convert.ToInt32(args[1]);
                    vocabulary = Convert.ToInt32(args[2]);
                    batchSize = Convert.ToInt32(args[3]);
                    iterations = Convert.ToInt32(args[4]);
                }

                var random = new Random(0);
                List<Document> docs = Enumerable.Range(0, documentCount)
                                               .Select(i => new Document(Enumerable.Range(0, 10)
                                               .Select(j => String.Format("{0}", random.Next(vocabulary)))
                                               .Aggregate((x, y) => x + " " + y), i)).ToList<Document>();

                List<Query>[] queryBatches = new List<Query>[iterations];

                for (int i = 0; i < iterations; i++)
                {
                    queryBatches[i] = Enumerable.Range(i * batchSize, batchSize)
                                                   .Select(j => new Query(String.Format("{0}", j % vocabulary), j, 1))
                                                   .ToList();
                }

                #endregion

                using (var manager = controller.NewGraph())
                {
                    // declare inputs for documents and queries.
                    var documents = new IncrementalCollection<Document>(manager);
                    var queries = new IncrementalCollection<Query>(manager);

                    // each document is broken down into a collection of terms, each with associated identifier.
                    var dTerms = documents.SelectMany(doc => doc.text.Split(' ').Select(term => new Document(term, doc.id)))
                                          .Distinct();

                    // each query is broken down into a collection of terms, each with associated identifier and threshold.
                    var qTerms = queries.SelectMany(query => query.text.Split(' ').Select(term => new Query(term, query.id, query.threshold)))
                                        .Distinct();

                    // doc terms and query terms are joined, matching pairs are counted and returned if the count exceeds the threshold.
                    var results = dTerms.Join(qTerms, d => d.text, q => q.text, (d, q) => new Match(d.id, q.id, q.threshold))
                                        .Count(match => match)
                                        .Select(pair => new Match(pair.v1.document, pair.v1.query, pair.v1.threshold - (int)pair.v2))
                                        .Where(match => match.threshold <= 0)
                                        .Select(match => new Pair<int, int>(match.document, match.query));

                    // subscribe to the output in case we are interested in the results
                    var subscription = results.Subscribe(list => Console.WriteLine("matches found: {0}", list.Length));

                    manager.Activate();

                    #region Prepare some fake documents to put in the collection

                    // creates many documents each containing 10 words from [0, ... vocabulary-1].
                    int share_size = docs.Count / controller.Configuration.Processes;

                    documents.OnNext(docs.GetRange(controller.Configuration.ProcessID * share_size, share_size));
                    queries.OnNext();

                    //Console.WriteLine("Example SearchIndex in Naiad. Step 1: indexing documents, step 2: issuing queries.");
                    Console.WriteLine("Indexing {0} random documents, {1} terms (please wait)", documentCount, 10 * documentCount);
                    subscription.Sync(0);

                    #endregion

                    #region Issue batches of queries and assess performance

                    Console.WriteLine("Issuing {0} rounds of batches of {1} queries (press [enter] to start)", iterations, batchSize);
                    Console.ReadLine();

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    for (int i = 0; i < iterations; i++)
                    {
                        // we round-robin through query terms. more advanced queries are possible.
                        if (controller.Configuration.ProcessID == 0)
                            queries.OnNext(queryBatches[i]); // introduce new queries.
                        else
                            queries.OnNext();

                        documents.OnNext();         // indicate no new docs.
                        subscription.Sync(i + 1);          // block until round is done.
                    }

                    documents.OnCompleted();
                    queries.OnCompleted();

                    controller.Join();

                    #endregion

                    manager.Join();
                }

                controller.Join();
            }
        }

        public string Usage { get { return ""; } }
    }
}
