using Raven.Client.Documents;
using Raven.TestDriver;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using System;

namespace RavenDBTestCase {
    public class RavenDbRqlIssue : RavenTestDriver {
        //This allows us to modify the conventions of the store we get from 'GetDocumentStore'
        protected override void PreInitialize( IDocumentStore documentStore ) {
            documentStore.Conventions.MaxNumberOfRequestsPerSession = 50;
            
        }

        public RavenDbRqlIssue() {
            try {
                ConfigureServer( new TestServerOptions {
                    DataDirectory = "C:\\RavenDBTestDir"
                } );
            } catch { }
        }

        private void SetupData( IDocumentStore store ) {
            using ( var session = store.OpenSession() ) {
                var i = 1;
                void AddUnit( string type, string category ) {
                    session.Store( new TestUnit { Name = $"Unit {i}", Type = type, Category = category } );
                    i++;
                }
                AddUnit( "Store", "A" );
                AddUnit( "Store", "X" );
                AddUnit( "Store", "X" );
                AddUnit( "Storage", "X" );
                AddUnit( "Storage", "A" );
                //5
                AddUnit( "Storage", "X" );
                AddUnit( "Booth", "X" );
                AddUnit( "Booth", "X" );
                AddUnit( "Booth", "X" );
                AddUnit( "Booth", "A" );
                //10
                AddUnit( "Trailer", "A" );
                AddUnit( "Trailer", "X" );
                AddUnit( "Trailer", "X" );
                AddUnit( "Trailer", "X" );

                session.SaveChanges();
            }
            WaitForIndexing( store ); //If we want to query documents sometime we need to wait for the indexes to catch up
        }

        private void SetupFact( Action<IDocumentSession> action ) {
            using ( var store = GetDocumentStore() ) {
                store.ExecuteIndex( new MapTestUnit() );

                SetupData( store );

                WaitForUserToContinueTheTest( store );//Sometimes we want to debug the test itself, this redirect us to the studio
                using ( var session = store.OpenSession() ) {
                    action( session );
                }
            }
        }

        [Fact]
        public void TestRQL_ShoudExcludeUnitsByName() {
            SetupFact( session => {
                var query = @"from index 'MapTestUnit' as m
                                  where Category = 'X'
                                  and NOT (Type in ('Booth'))
                                  and NOT (Name in ('Unit 4', 'Unit 12'))
                                ";

                var result = session.Advanced.RawQuery<TestUnit>( query )
                                   .ToList();

                Assert.Equal( 0, result.Count( x => x.Category == "A" ) );
                Assert.Equal( 0, result.Count( x => x.Type == "Booth" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 4" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 12" ) );
                Assert.Equal( 5, result.Count );
            } );
        }

        [Fact]
        public void TestRQL_ShouldExcludeUnitsByType() {
            SetupFact( session => {
                var query = @"from index 'MapTestUnit' as m
                                  where Category = 'X'
                                  and NOT (Name in ('Unit 4', 'Unit 12'))
                                  and NOT (Type in ('Booth'))
                                ";

                var result = session.Advanced.RawQuery<TestUnit>( query )
                                   .ToList();

                Assert.Equal( 0, result.Count( x => x.Category == "A" ) );
                Assert.Equal( 0, result.Count( x => x.Type == "Booth" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 4" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 12" ) );
                Assert.Equal( 5, result.Count );
            } );
        }

        [Fact]
        public void TestRQL_SequanceOfAndNotShouldNotAffectResultCount() {
            SetupFact( session => {
                var query = @"from index 'MapTestUnit' as m
                                  where Category = 'X'
                                  and NOT (Name in ('Unit 4', 'Unit 12'))
                                  and NOT (Type in ('Booth'))
                                ";

                var nameFirst = session.Advanced.RawQuery<TestUnit>( query )
                                   .ToList();

                var query1 = @"from index 'MapTestUnit' as m
                                  where Category = 'X'
                                  and NOT (Type in ('Booth'))
                                  and NOT (Name in ('Unit 4', 'Unit 12'))
                                ";

                var typeFirst = session.Advanced.RawQuery<TestUnit>( query1 )
                                   .ToList();

                Assert.Equal( nameFirst.Count, typeFirst.Count );
            } );
        }

        [Fact]
        public void TestLucene_ComplexQueryNotFiltering() {
            SetupFact( session => {
                var query = @"from index 'MapTestUnit' as m
                            where lucene( Category, 'Category: ""X"" AND NOT @in<Type>:(""Booth"") AND NOT @in<Name>:(""Unit 4"", ""Unit 12"")')";

                var result = session.Advanced.RawQuery<TestUnit>( query )
                                   .ToList();

                Assert.Equal( 0, result.Count( x => x.Category == "A" ) );
                Assert.Equal( 0, result.Count( x => x.Type == "Booth" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 4" ) );
                Assert.Equal( 0, result.Count( x => x.Name == "Unit 12" ) );
                Assert.Equal( 5, result.Count );
            } );
        }
    }

    public class MapTestUnit : AbstractIndexCreationTask<TestUnit> {
        public MapTestUnit() {
            Map = docs => from doc in docs select new { 
                doc.Name,
                doc.Category,
                doc.Type
            };
            
        }
    }

    public class TestUnit {
        public string Id { get; set; }
        public string Category { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }

    }
}