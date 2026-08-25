# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import numpy as np
from workloads.base import Workload
import gc
import time

class ResearchIntermediateOpt(Workload):
    NAME = "research_intermediate_opt"
    dataset_seed = 42

    # Configurable batch sizes - optimized for performance
    BATCH_SIZES = {
        'people': 15000,        # Increased from 4000
        'movies': 8000,         # Increased from 2000
        'connections': 3000,    # Increased from 2000
        'roles': 8000           # Increased from 250
    }

    def indexes_generator(self):
        print("\n" + "="*80)
        print("INDEXES: Creating database indexes...")
        print("="*80)
        indexes = []
        if "neo4j" in self.benchmark_context.vendor_name:
            indexes.extend([
                ("CREATE INDEX FOR (p:Person) ON (p.id);", {}),
                ("CREATE INDEX FOR (p:Person) ON (p.name);", {}),
                ("CREATE INDEX FOR (m:Movie) ON (m.id);", {}),
                ("CREATE INDEX FOR (m:Movie) ON (m.title);", {}),
                ("CREATE INDEX FOR (s:Studio) ON (s.name);", {}),
                ("CREATE INDEX FOR (g:Genre) ON (g.name);", {}),
                ("CREATE INDEX FOR (l:Language) ON (l.code);", {}),
                ("CREATE INDEX FOR (a:Award) ON (a.name);", {}),
                # Otimização específica deste workload
                ("CREATE INDEX FOR (a:ActorRole) ON (a.salary);", {}),
                ("CREATE INDEX FOR (a:ActorRole) ON (a.is_lead);", {}),
                ("CREATE INDEX FOR (a:ActorRole) ON (a.importance);", {}),
                ("CREATE INDEX FOR (a:ActorRole) ON (a.salary, a.is_lead);", {}),
                ("CREATE INDEX FOR (d:DirectorRole) ON (d.salary);", {}),
                ("CREATE INDEX FOR (pr:ProducerRole) ON (pr.salary);", {}),
                ("CREATE INDEX FOR (pr:ProducerRole) ON (pr.role);", {}),
                ("CREATE INDEX FOR (w:WriterRole) ON (w.salary);", {}),
                ("CREATE INDEX FOR (w:WriterRole) ON (w.credit);", {}),
                ("CREATE INDEX FOR (c:ComposerRole) ON (c.salary);", {}),
                ("CREATE INDEX FOR (c:ComposerRole) ON (c.award_nominated);", {}),
            ])
            print(f"  ✓ Creating {len(indexes)} Neo4j indexes")
        else:
            indexes.extend([
                ("CREATE INDEX ON :Person(id);", {}),
                ("CREATE INDEX ON :Person(name);", {}),
                ("CREATE INDEX ON :Movie(id);", {}),
                ("CREATE INDEX ON :Movie(title);", {}),
                ("CREATE INDEX ON :Studio(name);", {}),
                ("CREATE INDEX ON :Genre(name);", {}),
                ("CREATE INDEX ON :Language(code);", {}),
                ("CREATE INDEX ON :Award(name);", {}),
                # Otimização específica deste workload
                ("CREATE INDEX ON :ActorRole(salary);", {}),
                ("CREATE INDEX ON :ActorRole(is_lead);", {}),
                ("CREATE INDEX ON :ActorRole(importance);", {}),
                ("CREATE INDEX ON :ActorRole(salary, is_lead);", {}),
                ("CREATE INDEX ON :DirectorRole(salary);", {}),
                ("CREATE INDEX ON :ProducerRole(salary);", {}),
                ("CREATE INDEX ON :ProducerRole(role);", {}),
                ("CREATE INDEX ON :WriterRole(salary);", {}),
                ("CREATE INDEX ON :WriterRole(credit);", {}),
                ("CREATE INDEX ON :ComposerRole(salary);", {}),
                ("CREATE INDEX ON :ComposerRole(award_nominated);", {}),
            ])
            print(f"  ✓ Creating {len(indexes)} Memgraph indexes")
        print("="*80 + "\n")
        return indexes

    def dataset_generator(self):
        print("\n" + "="*80)
        print("DATASET GENERATION STARTED")
        print("="*80)
        
        start_time = time.time()
        np.random.seed(self.dataset_seed)
        scale = 1 #10 for large
        
        studios = [f"Studio_{i}" for i in range(50)]
        genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Romance", "Thriller"]
        languages = ["en", "fr", "de", "ja", "es", "zh", "ko"]
        countries = ["US", "UK", "FR", "DE", "JP", "CA", "AU", "KR", "IT", "BR"]
        awards = ["Oscar", "Golden_Globe", "BAFTA", "Cannes"]

        total_people = 20000 * scale
        total_movies = 8000 * scale
        total_roles = 100000 * scale
        
        print(f"\n📊 Dataset Statistics (scale={scale}):")
        print(f"  • People: {total_people:,}")
        print(f"  • Movies: {total_movies:,}")
        print(f"  • Roles: {total_roles:,}")
        print(f"  • Studios: {len(studios)}")
        print(f"  • Genres: {len(genres)}")
        print(f"  • Languages: {len(languages)}")
        print(f"  • Countries: {len(countries)}")
        print(f"  • Awards: {len(awards)}")
        print("="*80)

        weights_array = np.power((total_people - np.arange(total_people)).astype(np.float64), 0.7)
        
        # Batch: Create studios
        print("\n🏢 STUDIOS: Creating studios...")
        studio_start = time.time()
        studio_queries = []
        for i, studio in enumerate(studios):
            studio_queries.append((
                "CREATE (:Studio {id: $id, name: $name, founded: $year, country: $country});",
                {"id": i, "name": studio, "year": int(np.random.randint(1920, 2020)), 
                 "country": np.random.choice(countries)}
            ))
        yield studio_queries
        print(f"  ✓ Created {len(studios)} studios in {time.time() - studio_start:.2f}s")
        
        # Batch: Create genres
        print("\n🎭 GENRES: Creating genres...")
        genre_start = time.time()
        genre_queries = []
        for genre in genres:
            genre_queries.append(("CREATE (:Genre {name: $name});", {"name": genre}))
        yield genre_queries
        print(f"  ✓ Created {len(genres)} genres in {time.time() - genre_start:.2f}s")
        
        # Batch: Create languages
        print("\n🌐 LANGUAGES: Creating languages...")
        lang_start = time.time()
        lang_queries = []
        for lang in languages:
            lang_queries.append(("CREATE (:Language {code: $code, name: $name});", 
                              {"code": lang, "name": f"Language_{lang}"}))
        yield lang_queries
        print(f"  ✓ Created {len(languages)} languages in {time.time() - lang_start:.2f}s")
        
        # Batch: Create awards
        print("\n🏆 AWARDS: Creating awards...")
        award_start = time.time()
        award_queries = []
        for award in awards:
            award_queries.append(("CREATE (:Award {name: $name, prestige: $prestige});", 
                                {"name": award, "prestige": int(np.random.randint(1, 100))}))
        yield award_queries
        print(f"  ✓ Created {len(awards)} awards in {time.time() - award_start:.2f}s")
        
        # Batch: Create people in batches - INCREASED BATCH SIZE
        people_per_batch = self.BATCH_SIZES['people']
        num_people_batches = int(np.ceil(total_people / people_per_batch))
        
        print(f"\n👤 PEOPLE: Creating {total_people:,} people in {num_people_batches} batches of {people_per_batch:,}")
        print("-" * 60)
        
        top_1_percent = int(0.01 * total_people)
        top_10_percent = int(0.1 * total_people)

        people_start = time.time()
        for batch_num in range(num_people_batches):
            batch_start_time = time.time()
            people_queries = []
            batch_start = batch_num * people_per_batch
            batch_end = min(batch_start + people_per_batch, total_people)
            batch_size = batch_end - batch_start
            
            print(f"  Batch {batch_num + 1:>3}/{num_people_batches}: People {batch_start:>7,} to {batch_end:>7,} ({batch_size:>5,} people) ", end="", flush=True)
            
            for i in range(batch_start, batch_end):
                if i < top_1_percent: 
                    popularity = int(np.random.randint(80, 100))
                elif i < top_10_percent: 
                    popularity = int(np.random.randint(30, 79))
                else:  
                    popularity = int(np.random.randint(1, 29))
                    
                people_queries.append((
                    "CREATE (:Person {id: $id, name: $name, country: $country, birth_year: $year, popularity: $pop});",
                    {"id": i, "name": f"Person_{i}", "country": np.random.choice(countries), 
                     "year": int(np.random.randint(1940, 2000)), "pop": popularity}
                ))
            yield people_queries
            
            batch_time = time.time() - batch_start_time
            print(f"✓ {batch_time:.2f}s")
            
            # Periodic garbage collection for large batches
            if batch_num % 10 == 0:
                gc.collect()
        
        print(f"  ✓ Total people creation time: {time.time() - people_start:.2f}s")
        
        # Batch: Create movies in batches - INCREASED BATCH SIZE
        movies_per_batch = self.BATCH_SIZES['movies']
        num_movie_batches = int(np.ceil(total_movies / movies_per_batch))
        
        print(f"\n🎬 MOVIES: Creating {total_movies:,} movies in {num_movie_batches} batches of {movies_per_batch:,}")
        print("-" * 60)
        
        top_1_percent_movies = int(0.01 * total_movies)
        top_10_percent_movies = int(0.10 * total_movies)

        movies_start = time.time()
        for batch_num in range(num_movie_batches):
            batch_start_time = time.time()
            movie_queries = []
            batch_start = batch_num * movies_per_batch
            batch_end = min(batch_start + movies_per_batch, total_movies)
            batch_size = batch_end - batch_start
            
            print(f"  Batch {batch_num + 1:>3}/{num_movie_batches}: Movies {batch_start:>7,} to {batch_end:>7,} ({batch_size:>5,} movies) ", end="", flush=True)
            
            for i in range(batch_start, batch_end):
                year = int(np.random.randint(1980, 2023))

                if i < top_1_percent_movies:
                    budget = int(np.random.randint(100000000, 300000000))
                elif i < top_10_percent_movies: 
                    budget = int(np.random.randint(20000000, 99999999))
                else:
                    budget = int(np.random.randint(1000000, 19999999))
                    
                revenue = budget * np.random.uniform(0.1, 20.0)
                
                movie_queries.append((
                    """CREATE (:Movie {id: $id, title: $title, year: $year, budget: $budget, 
                           revenue: $revenue, rating: $rating, runtime: $runtime, votes: $votes});""",
                    {"id": i, "title": f"Movie_{i}", "year": year, "budget": budget, 
                     "revenue": revenue, "rating": round(np.random.uniform(1.0, 10.0), 1), 
                     "runtime": int(np.random.randint(70, 210)), "votes": int(np.random.randint(1000, 1000000))}
                ))
            yield movie_queries
            
            batch_time = time.time() - batch_start_time
            print(f"✓ {batch_time:.2f}s")
            
            # Periodic garbage collection
            if batch_num % 10 == 0:
                gc.collect()
        
        print(f"  ✓ Total movies creation time: {time.time() - movies_start:.2f}s")
        
        # Batch: Creates the connections between movies and other nodes - INCREASED BATCH SIZE
        connections_per_batch = self.BATCH_SIZES['connections']
        num_connection_batches = int(np.ceil(total_movies / connections_per_batch))
        
        print(f"\n🔗 CONNECTIONS: Creating connections for {total_movies:,} movies in {num_connection_batches} batches of {connections_per_batch:,}")
        print("-" * 60)
        
        connections_start = time.time()
        for batch_num in range(num_connection_batches):
            batch_start_time = time.time()
            connection_queries = []
            batch_start = batch_num * connections_per_batch
            batch_end = min(batch_start + connections_per_batch, total_movies)
            batch_size = batch_end - batch_start
            
            print(f"  Batch {batch_num + 1:>3}/{num_connection_batches}: Movies {batch_start:>7,} to {batch_end:>7,} ({batch_size:>5,} movies) ", end="", flush=True)
            
            for i in range(batch_start, batch_end):
                studio_weights = np.array([50 if j < 5 else 1 for j in range(50)])
                studio_weights = studio_weights / np.sum(studio_weights)  #Normalização
                studio_id = int(np.random.choice(range(50), p=studio_weights))
                
                connection_queries.append((
                    "MATCH (m:Movie {id: $id}), (s:Studio {id: $studio_id}) CREATE (m)-[:PRODUCED_BY]->(s);",
                    {"id": i, "studio_id": studio_id}
                ))
                
                num_genres = int(np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
                for _ in range(num_genres):
                    connection_queries.append((
                        "MATCH (m:Movie {id: $id}), (g:Genre {name: $genre}) CREATE (m)-[:HAS_GENRE]->(g);",
                        {"id": i, "genre": np.random.choice(genres)}
                    ))
                
                connection_queries.append((
                    "MATCH (m:Movie {id: $id}), (l:Language {code: $lang}) CREATE (m)-[:IN_LANGUAGE]->(l);",
                    {"id": i, "lang": np.random.choice(languages)}
                ))
                
                if np.random.random() < 0.3:
                    num_awards = int(np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1]))
                    award_year = int(np.random.randint(1980, 2024))
                    for _ in range(num_awards):
                        connection_queries.append((
                            "MATCH (m:Movie {id: $id}), (a:Award {name: $award}) CREATE (m)-[:WON {year: $year}]->(a);",
                            {"id": i, "award": np.random.choice(awards), "year": award_year}
                        ))
            yield connection_queries
            
            batch_time = time.time() - batch_start_time
            print(f"✓ {batch_time:.2f}s ({len(connection_queries):,} queries)")
            
            # Periodic garbage collection
            if batch_num % 10 == 0:
                gc.collect()
        
        print(f"  ✓ Total connections creation time: {time.time() - connections_start:.2f}s")
        
        # Batch: Create specialized intermediate nodes - SIGNIFICANTLY INCREASED BATCH SIZE
        roles_per_batch = self.BATCH_SIZES['roles']
        num_role_batches = int(np.ceil(total_roles / roles_per_batch))

        print(f"\n🎭 ROLES: Generating {total_roles:,} specialized role nodes")
        print(f"  • {num_role_batches} batches of {roles_per_batch:,} roles each")
        print("-" * 60)

        roles_start = time.time()
        role_counts = {"ACTOR": 0, "DIRECTOR": 0, "PRODUCER": 0, "WRITER": 0, "COMPOSER": 0}
        
        for batch_num in range(num_role_batches):
            batch_start_time = time.time()
            batch_start = batch_num * roles_per_batch
            batch_end = min(batch_start + roles_per_batch, total_roles)
            batch_size = batch_end - batch_start

            print(f"  Batch {batch_num + 1:>3}/{num_role_batches}: Roles {batch_start:>7,} to {batch_end:>7,} ({batch_size:>5,} roles) ", end="", flush=True)

            role_data = {"ACTOR": [], "DIRECTOR": [], "PRODUCER": [], "WRITER": [], "COMPOSER": []}
            
            for i in range(batch_start, batch_end):
                person_id = int(np.random.choice(
                    range(total_people),
                    p=weights_array/np.sum(weights_array)
                ))
                
                role_type = np.random.choice(["ACTOR", "DIRECTOR", "PRODUCER", "WRITER", "COMPOSER"])
                role_counts[role_type] += 1
                movie_id = int(np.random.randint(0, total_movies - 1))

                base_salary = {
                    "ACTOR": int(np.random.randint(10000, 5000000)), 
                    "DIRECTOR": int(np.random.randint(500000, 5000000)), 
                    "PRODUCER": int(np.random.randint(300000, 3000000)),  
                    "WRITER": int(np.random.randint(50000, 1000000)),  
                    "COMPOSER": int(np.random.randint(20000, 500000))  
                }[role_type]
                
                popularity_factor = (person_id % 100 + 1) / 100.0
                salary = int(base_salary * (0.5 + popularity_factor * 1.5))
                
                if role_type == "ACTOR":
                    role_data["ACTOR"].append({
                        "p_id": person_id, "m_id": movie_id, "char": f"Character_{i}",
                        "salary": salary, "time": int(np.random.randint(5, 180)),  
                        "lead": np.random.random() < 0.1, "imp": np.random.choice(["lead", "supporting", "cameo"]) 
                    })
                elif role_type == "DIRECTOR":
                    role_data["DIRECTOR"].append({
                        "p_id": person_id, "m_id": movie_id, "salary": salary
                    })
                elif role_type == "PRODUCER":
                    role_data["PRODUCER"].append({
                        "p_id": person_id, "m_id": movie_id, "salary": salary,
                        "role": np.random.choice(["executive", "line", "associate"])  
                    })
                elif role_type == "WRITER":
                    role_data["WRITER"].append({
                        "p_id": person_id, "m_id": movie_id, "salary": salary,
                        "credit": np.random.choice(["screenplay", "story", "dialogue"])  
                    })
                else:  # COMPOSER
                    role_data["COMPOSER"].append({
                        "p_id": person_id, "m_id": movie_id, "salary": salary,
                        "award": np.random.random() < 0.05  
                    })

            # Create UNWIND queries for each role type
            role_queries = []
            
            if role_data["ACTOR"]:
                role_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id}) 
                    CREATE (p)-[:PERFORMED_AS]->(a:ActorRole {
                        character: row.char, salary: row.salary, screen_time: row.time, 
                        is_lead: row.lead, importance: row.imp
                    })-[:FOR_MOVIE]->(m)
                    """,
                    {"data": role_data["ACTOR"]}
                ))
            
            if role_data["DIRECTOR"]:
                role_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id}) 
                    CREATE (p)-[:DIRECTED_AS]->(d:DirectorRole {salary: row.salary})-[:FOR_MOVIE]->(m)
                    """,
                    {"data": role_data["DIRECTOR"]}
                ))
            
            if role_data["PRODUCER"]:
                role_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id}) 
                    CREATE (p)-[:PRODUCED_AS]->(pr:ProducerRole {
                        salary: row.salary, role: row.role
                    })-[:FOR_MOVIE]->(m)
                    """,
                    {"data": role_data["PRODUCER"]}
                ))
            
            if role_data["WRITER"]:
                role_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id}) 
                    CREATE (p)-[:WROTE_AS]->(w:WriterRole {
                        salary: row.salary, credit: row.credit
                    })-[:FOR_MOVIE]->(m)
                    """,
                    {"data": role_data["WRITER"]}
                ))
            
            if role_data["COMPOSER"]:
                role_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id}) 
                    CREATE (p)-[:COMPOSED_AS]->(c:ComposerRole {
                        salary: row.salary, award_nominated: row.award
                    })-[:FOR_MOVIE]->(m)
                    """,
                    {"data": role_data["COMPOSER"]}
                ))
            
            yield role_queries
            
            batch_time = time.time() - batch_start_time
            queries_count = len(role_queries)
            total_roles_in_batch = sum(len(data) for data in role_data.values())
            print(f"✓ {batch_time:.2f}s ({queries_count} UNWIND queries, {total_roles_in_batch:,} roles)")
            
            # Clean up to free memory
            del role_data
            del role_queries

            # More frequent garbage collection for larger batches
            if batch_num % 5 == 0:
                gc.collect()
        
        print(f"  ✓ Total roles creation time: {time.time() - roles_start:.2f}s")

        # Final summary
        total_time = time.time() - start_time
        print("\n" + "="*80)
        print("✅ DATASET GENERATION COMPLETE")
        print("="*80)
        print(f"  • Total time: {total_time:.2f}s")
        print(f"  • People: {total_people:,}")
        print(f"  • Movies: {total_movies:,}")
        print(f"  • Roles: {total_roles:,}")
        print(f"  • Batches processed: {num_people_batches + num_movie_batches + num_connection_batches + num_role_batches}")
        print(f"\n📊 Role breakdown:")
        print(f"  • ACTOR: {role_counts['ACTOR']:,}")
        print(f"  • DIRECTOR: {role_counts['DIRECTOR']:,}")
        print(f"  • PRODUCER: {role_counts['PRODUCER']:,}")
        print(f"  • WRITER: {role_counts['WRITER']:,}")
        print(f"  • COMPOSER: {role_counts['COMPOSER']:,}")
        print("="*80 + "\n")

    def benchmark__test__strong_collaboration_clusters(self):
        min_collaborations = 2
        print("\n🔍 Running: Strong Collaboration Clusters")
        print(f"  • min_collaborations: {min_collaborations}")
        return ("""
        // OPTIMIZED: Encontra clusters de colaboração usando estrutura intermediária
        MATCH (a:Person)-[]->(role1)-[:FOR_MOVIE]->(m1:Movie)<-[:FOR_MOVIE]-(role2)<-[]-(b:Person)
        WHERE a.id < b.id
        WITH a, b, COUNT(DISTINCT m1) as ab_strength
        WHERE ab_strength >= $min_collaborations
        
        MATCH (b)-[]->(role3)-[:FOR_MOVIE]->(m2:Movie)<-[:FOR_MOVIE]-(role4)<-[]-(c:Person)
        WHERE c.id > b.id AND c <> a
        WITH a, b, c, ab_strength, COUNT(DISTINCT m2) as bc_strength
        WHERE bc_strength >= $min_collaborations
        
        MATCH (c)-[]->(role5)-[:FOR_MOVIE]->(m3:Movie)<-[:FOR_MOVIE]-(role6)<-[]-(a)
        WITH a, b, c, ab_strength, bc_strength, COUNT(DISTINCT m3) as ca_strength
        WHERE ca_strength >= $min_collaborations
        
        RETURN a.name as person1, b.name as person2, c.name as person3,
            (ab_strength + bc_strength + ca_strength) as total_cluster_strength
        ORDER BY total_cluster_strength DESC
        LIMIT 15;
        """, {"min_collaborations": min_collaborations})
        
    def benchmark__test__complex_categorical_analytics(self):
        target_genres = ["Action", "Drama"]
        min_year = 2008
        min_rating = 7.2
        
        print("\n🔍 Running: Complex Categorical Analytics")
        print(f"  • target_genres: {target_genres}")
        print(f"  • min_year: {min_year}")
        print(f"  • min_rating: {min_rating}")
        
        return ("""
        // Complex multi-category business intelligence query
        MATCH (m:Movie)
        WHERE m.year >= $min_year 
          AND m.rating >= $min_rating
        WITH m
        MATCH (m)-[:PRODUCED_BY]->(s:Studio)
        MATCH (m)-[:HAS_GENRE]->(g:Genre)
        WHERE g.name IN $genres
        MATCH (m)-[:IN_LANGUAGE]->(l:Language)
        WITH s, g, l,
             COUNT(m) as movie_count,
             AVG(m.budget) as avg_budget,
             AVG(m.revenue) as avg_revenue,
             AVG(m.rating) as avg_rating
        WHERE movie_count >= 3
        RETURN s.name as studio, g.name as genre, l.name as language,
               movie_count, avg_budget, avg_revenue, avg_rating
        ORDER BY avg_revenue DESC
        LIMIT 20;
        """, {"min_year": min_year, "min_rating": min_rating, "genres": target_genres})
    
    def benchmark__test__cross_role_workforce_analysis(self):
        """CORRECT: Already schema-aware - uses specialized intermediate nodes"""
        print("\n🔍 Running: Cross-Role Workforce Analysis")
        return ("""
        // Specialized nodes require UNION but each branch is optimized
        MATCH (p:Person)-[:PERFORMED_AS]->(a:ActorRole)-[:FOR_MOVIE]->(m:Movie)
        RETURN p.name, 'ACTOR' as role, a.salary as salary
        UNION
        MATCH (p:Person)-[:DIRECTED_AS]->(d:DirectorRole)-[:FOR_MOVIE]->(m:Movie)  
        RETURN p.name, 'DIRECTOR' as role, d.salary as salary
        UNION
        MATCH (p:Person)-[:PRODUCED_AS]->(pr:ProducerRole)-[:FOR_MOVIE]->(m:Movie)
        RETURN p.name, 'PRODUCER' as role, pr.salary as salary
        UNION
        MATCH (p:Person)-[:WROTE_AS]->(w:WriterRole)-[:FOR_MOVIE]->(m:Movie)
        RETURN p.name, 'WRITER' as role, w.salary as salary
        UNION
        MATCH (p:Person)-[:COMPOSED_AS]->(c:ComposerRole)-[:FOR_MOVIE]->(m:Movie)
        RETURN p.name, 'COMPOSER' as role, c.salary as salary
        ORDER BY salary DESC
        LIMIT 30;
        """, {})
    
    def benchmark__test__relationship_property_mining(self):
        """CORRECT: Already schema-aware - uses ActorRole properties"""
        min_salary = 1500000
        
        print("\n🔍 Running: Relationship Property Mining")
        print(f"  • min_salary: ${min_salary:,}")
        
        return ("""
        // FAST: Can use specialized indexes on ActorRole properties
        MATCH (p:Person)-[:PERFORMED_AS]->(a:ActorRole {is_lead: true})
        WHERE a.salary >= $min_salary
        WITH p, a
        MATCH (p)-[:PERFORMED_AS]->(all_a:ActorRole)
        WITH p, 
             COUNT(DISTINCT a) as high_paid_lead_roles,
             AVG(a.salary) as avg_lead_salary,
             COUNT(DISTINCT all_a) as total_acting_roles,
             AVG(all_a.salary) as overall_avg_salary
        RETURN p.name, p.popularity, 
               high_paid_lead_roles, avg_lead_salary, 
               total_acting_roles, overall_avg_salary
        ORDER BY avg_lead_salary DESC
        LIMIT 20;
        """, {"min_salary": min_salary})
    
    def benchmark__test__workforce_salary_analytics(self):
        """CORRECT: Already schema-aware - uses specialized node indexes"""
        min_salary = 1000000
        
        print("\n🔍 Running: Workforce Salary Analytics")
        print(f"  • min_salary: ${min_salary:,}")
        
        return ("""
        // Analytics across all roles using specialized indexes
        MATCH (p:Person)-[:PERFORMED_AS]->(a:ActorRole)
        WHERE a.salary >= $min_salary
        WITH p, 'ACTOR' as role, COUNT(*) as role_count, AVG(a.salary) as avg_salary, MAX(a.salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        
        UNION ALL
        
        MATCH (p:Person)-[:DIRECTED_AS]->(d:DirectorRole)
        WHERE d.salary >= $min_salary
        WITH p, 'DIRECTOR' as role, COUNT(*) as role_count, AVG(d.salary) as avg_salary, MAX(d.salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        
        UNION ALL
        
        MATCH (p:Person)-[:PRODUCED_AS]->(pr:ProducerRole)
        WHERE pr.salary >= $min_salary
        WITH p, 'PRODUCER' as role, COUNT(*) as role_count, AVG(pr.salary) as avg_salary, MAX(pr.salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        
        UNION ALL
        
        MATCH (p:Person)-[:WROTE_AS]->(w:WriterRole)
        WHERE w.salary >= $min_salary
        WITH p, 'WRITER' as role, COUNT(*) as role_count, AVG(w.salary) as avg_salary, MAX(w.salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        
        UNION ALL
        
        MATCH (p:Person)-[:COMPOSED_AS]->(c:ComposerRole)
        WHERE c.salary >= $min_salary
        WITH p, 'COMPOSER' as role, COUNT(*) as role_count, AVG(c.salary) as avg_salary, MAX(c.salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        
        ORDER BY avg_salary DESC
        LIMIT 20;
        """, {"min_salary": min_salary})
    
    def benchmark__test__denormalized_genre_performance(self):
        """VERSÃO NORMALIZADA - Query equivalente mas sem desnormalização"""
        print("\n🔍 Running: Denormalized Genre Performance (Normalized version)")
        return ("""
        // VERSÃO NORMALIZADA - Requer cálculo em tempo real
        MATCH (g:Genre)<-[:HAS_GENRE]-(m:Movie)
        WITH g, 
            AVG(m.rating) as avg_rating,
            AVG(m.budget) as avg_budget,
            COUNT(m) as movie_count
        WHERE avg_budget >= 20000000 
        AND avg_rating >= 7.0
        RETURN g.name, avg_rating, avg_budget, movie_count
        ORDER BY avg_rating DESC, avg_budget DESC
        LIMIT 10;
        """, {})
    
    def benchmark__test__complex_country_network_intermediate(self):
        """INTERMEDIATE VERSION - Uses intermediate nodes but still property scans"""
        print("\n🔍 Running: Complex Country Network (Intermediate version)")
        return ("""
        // Find countries with strong domestic collaboration networks
        MATCH (p1:Person)-[:PERFORMED_AS]->(a1:ActorRole)-[:FOR_MOVIE]->(m:Movie)<-[:FOR_MOVIE]-(a2:ActorRole)<-[:PERFORMED_AS]-(p2:Person)
        WHERE p1.country = p2.country  // STILL EXPENSIVE: Property comparison
        WITH p1.country as country_code, 
            COUNT(DISTINCT m) as domestic_movies,
            COUNT(DISTINCT p1) as unique_actors
        
        // Find cross-country collaborations for these countries  
        MATCH (p3:Person)-[:PERFORMED_AS]->(a3:ActorRole)-[:FOR_MOVIE]->(m2:Movie)<-[:FOR_MOVIE]-(a4:ActorRole)<-[:PERFORMED_AS]-(p4:Person)
        WHERE p3.country = country_code AND p4.country <> country_code  // MORE property scans
        
        WITH country_code, domestic_movies, unique_actors,
            COUNT(DISTINCT m2) as intl_movies,
            COUNT(DISTINCT p4.country) as partner_countries
        
        WHERE domestic_movies >= 100 
        AND intl_movies >= 50
        AND unique_actors >= 200
        
        RETURN country_code, domestic_movies, intl_movies, 
            unique_actors, partner_countries,
            (domestic_movies * 1.0 / unique_actors) as collaboration_density
        ORDER BY collaboration_density DESC, partner_countries DESC
        LIMIT 10;
        """, {})