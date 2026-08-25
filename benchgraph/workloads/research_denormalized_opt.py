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

class ResearchDenormalizedOpt(Workload):
    NAME = "research_denormalized_opt"
    dataset_seed = 42

    # Configurable batch sizes - optimized for performance
    BATCH_SIZES = {
        'people': 15000,        # Increased from 4000
        'movies': 8000,         # Increased from 2000
        'connections': 3000,    # Increased from 2000
        'relationships': 8000   # Increased from 250
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
                ("CREATE INDEX FOR (p:Person) ON (p.avg_lead_salary, p.lead_roles_count);", {}),
                ("CREATE INDEX FOR (g:Genre) ON (g.avg_rating, g.avg_budget);", {}),
                ("CREATE INDEX FOR (l:Language) ON (l.avg_revenue);", {}),
                ("CREATE INDEX FOR (s:Studio) ON (s.avg_budget, s.total_revenue);", {}),
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
                ("CREATE INDEX ON :Person(avg_lead_salary, lead_roles_count);", {}),
                ("CREATE INDEX ON :Genre(avg_rating, avg_budget);", {}),
                ("CREATE INDEX ON :Language(avg_revenue);", {}),
                ("CREATE INDEX ON :Studio(avg_budget, total_revenue);", {}),
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
        scale = 20
        
        studios = [f"Studio_{i}" for i in range(50)]
        genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Romance", "Thriller"]
        languages = ["en", "fr", "de", "ja", "es", "zh", "ko"]
        countries = ["US", "UK", "FR", "DE", "JP", "CA", "AU", "KR", "IT", "BR"]
        awards = ["Oscar", "Golden_Globe", "BAFTA", "Cannes"]
        
        total_people = 20000 * scale
        total_movies = 8000 * scale
        total_relationships = 100000 * scale
        
        print(f"\n📊 Dataset Statistics (scale={scale}):")
        print(f"  • People: {total_people:,}")
        print(f"  • Movies: {total_movies:,}")
        print(f"  • Relationships: {total_relationships:,}")
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
            genre_queries.append((
                "CREATE (:Genre {name: $name});",
                {"name": genre}
            ))
        yield genre_queries
        print(f"  ✓ Created {len(genres)} genres in {time.time() - genre_start:.2f}s")
        
        # Batch: Create languages 
        print("\n🌐 LANGUAGES: Creating languages...")
        lang_start = time.time()
        lang_queries = []
        for lang in languages:
            lang_queries.append((
                "CREATE (:Language {code: $code, name: $name});", 
                {"code": lang, "name": f"Language_{lang}"}
            ))
        yield lang_queries
        print(f"  ✓ Created {len(languages)} languages in {time.time() - lang_start:.2f}s")
        
        # Batch: Create awards
        print("\n🏆 AWARDS: Creating awards...")
        award_start = time.time()
        award_queries = []
        for award in awards:
            award_queries.append((
                "CREATE (:Award {name: $name, prestige: $prestige});", 
                {"name": award, "prestige": int(np.random.randint(1, 100))}
            ))
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
                studio_weights = studio_weights / np.sum(studio_weights)
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
        
        # Batch: Create relationships - SIGNIFICANTLY INCREASED BATCH SIZE
        relationships_per_batch = self.BATCH_SIZES['relationships']
        num_relationship_batches = int(np.ceil(total_relationships / relationships_per_batch))

        print(f"\n🔗 RELATIONSHIPS: Generating {total_relationships:,} relationships")
        print(f"  • {num_relationship_batches} batches of {relationships_per_batch:,} relationships each")
        print("-" * 60)

        relationships_start = time.time()
        role_counts = {"ACTOR": 0, "DIRECTOR": 0, "PRODUCER": 0, "WRITER": 0, "COMPOSER": 0}
        
        for batch_num in range(num_relationship_batches):
            batch_start_time = time.time()
            batch_start = batch_num * relationships_per_batch
            batch_end = min(batch_start + relationships_per_batch, total_relationships)
            batch_size = batch_end - batch_start

            print(f"  Batch {batch_num + 1:>3}/{num_relationship_batches}: Relations {batch_start:>7,} to {batch_end:>7,} ({batch_size:>5,} relations) ", end="", flush=True)

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
                        "p_id": person_id, "m_id": movie_id, "salary": salary,
                        "char": f"Character_{i}", "time": int(np.random.randint(5, 180)),
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

            relationship_queries = []
            
            if role_data["ACTOR"]:
                relationship_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id})
                    CREATE (p)-[:ACTED_IN {
                        character: row.char, 
                        salary: row.salary,
                        screen_time: row.time, 
                        is_lead: row.lead, 
                        importance: row.imp
                    }]->(m)
                    """,
                    {"data": role_data["ACTOR"]}
                ))
            
            if role_data["DIRECTOR"]:
                relationship_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id})
                    CREATE (p)-[:DIRECTED {salary: row.salary}]->(m)
                    """,
                    {"data": role_data["DIRECTOR"]}
                ))
            
            if role_data["PRODUCER"]:
                relationship_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id})
                    CREATE (p)-[:PRODUCED {salary: row.salary, role: row.role}]->(m)
                    """,
                    {"data": role_data["PRODUCER"]}
                ))
            
            if role_data["WRITER"]:
                relationship_queries.append((
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id})
                    CREATE (p)-[:WROTE {salary: row.salary, credit: row.credit}]->(m)
                    """,
                    {"data": role_data["WRITER"]}
                ))
            
            if role_data["COMPOSER"]:
                relationship_queries.append(( 
                    """
                    UNWIND $data as row
                    MATCH (p:Person {id: row.p_id}), (m:Movie {id: row.m_id})
                    CREATE (p)-[:COMPOSED_FOR {salary: row.salary, award_nominated: row.award}]->(m)
                    """,
                    {"data": role_data["COMPOSER"]}
                ))
            
            yield relationship_queries
            
            batch_time = time.time() - batch_start_time
            queries_count = len(relationship_queries)
            total_relations_in_batch = sum(len(data) for data in role_data.values())
            print(f"✓ {batch_time:.2f}s ({queries_count} UNWIND queries, {total_relations_in_batch:,} relations)")
            
            # Clean up to free memory
            del role_data
            del relationship_queries

            # More frequent garbage collection for larger batches
            if batch_num % 5 == 0:
                gc.collect()
        
        print(f"  ✓ Total relationships creation time: {time.time() - relationships_start:.2f}s")

        print("\n" + "="*80)
        print("📊 COMPUTING DENORMALIZED STATISTICS")
        print("="*80)
        
        denorm_start = time.time()
        denormalization_queries = []

        print("  • Computing Person statistics (ACTED_IN relationships)...")
        denormalization_queries.append((
            """MATCH (p:Person)-[r:ACTED_IN]->(:Movie)
            WITH p,
                 COUNT(r) as total_acting_roles,
                 SUM(CASE WHEN r.is_lead THEN 1 ELSE 0 END) as lead_roles_count,
                 AVG(r.salary) as avg_acting_salary,
                 MAX(r.salary) as max_acting_salary,
                 AVG(CASE WHEN r.is_lead THEN r.salary ELSE null END) as avg_lead_salary
            SET p.total_acting_roles = total_acting_roles,
                p.lead_roles_count = lead_roles_count,
                p.avg_acting_salary = avg_acting_salary,
                p.max_acting_salary = max_acting_salary,
                p.avg_lead_salary = avg_lead_salary;""",
            {}
        ))
        print("  ✓ Person statistics query prepared")

        print("  • Computing Genre statistics...")
        denormalization_queries.append((
            """MATCH (g:Genre)<-[:HAS_GENRE]-(m:Movie)
            WITH g, 
                 COUNT(m) as movie_count,
                 AVG(m.rating) as avg_rating,
                 AVG(m.budget) as avg_budget,
                 AVG(m.revenue) as avg_revenue,
                 SUM(m.revenue) as total_revenue
            SET g.movie_count = movie_count,
                g.avg_rating = avg_rating,
                g.avg_budget = avg_budget,
                g.avg_revenue = avg_revenue,
                g.total_revenue = total_revenue;""",
            {}
        ))
        print("  ✓ Genre statistics query prepared")

        print("  • Computing Language statistics...")
        denormalization_queries.append((
            """MATCH (l:Language)<-[:IN_LANGUAGE]-(m:Movie)
            WITH l,
                 COUNT(m) as movie_count,
                 AVG(m.revenue) as avg_revenue,
                 AVG(m.rating) as avg_rating,
                 AVG(m.budget) as avg_budget
            SET l.movie_count = movie_count,
                l.avg_revenue = avg_revenue,
                l.avg_rating = avg_rating,
                l.avg_budget = avg_budget;""",
            {}
        ))
        print("  ✓ Language statistics query prepared")

        print("  • Computing Studio statistics...")
        denormalization_queries.append((
            """MATCH (s:Studio)<-[:PRODUCED_BY]-(m:Movie)
            WITH s,
                 COUNT(m) as movie_count,
                 AVG(m.budget) as avg_budget,
                 AVG(m.revenue) as avg_revenue,
                 SUM(m.revenue) as total_revenue,
                 AVG(m.rating) as avg_rating
            SET s.movie_count = movie_count,
                s.avg_budget = avg_budget,
                s.avg_revenue = avg_revenue,
                s.total_revenue = total_revenue,
                s.avg_rating = avg_rating;""",
            {}
        ))
        print("  ✓ Studio statistics query prepared")

        print("  • Computing Director statistics...")
        denormalization_queries.append((
            """MATCH (p:Person)-[r:DIRECTED]->(:Movie)
            WITH p,
                 COUNT(r) as total_directed,
                 AVG(r.salary) as avg_directing_salary,
                 MAX(r.salary) as max_directing_salary
            SET p.total_directed = total_directed,
                p.avg_directing_salary = avg_directing_salary,
                p.max_directing_salary = max_directing_salary;""",
            {}
        ))
        print("  ✓ Director statistics query prepared")

        yield denormalization_queries
        
        print(f"  ✓ All denormalization queries completed in {time.time() - denorm_start:.2f}s")

        # Final summary
        total_time = time.time() - start_time
        print("\n" + "="*80)
        print("✅ DATASET GENERATION COMPLETE")
        print("="*80)
        print(f"  • Total time: {total_time:.2f}s")
        print(f"  • People: {total_people:,}")
        print(f"  • Movies: {total_movies:,}")
        print(f"  • Relationships: {total_relationships:,}")
        print(f"  • Batches processed: {num_people_batches + num_movie_batches + num_connection_batches + num_relationship_batches}")
        print(f"\n📊 Relationship breakdown:")
        print(f"  • ACTOR: {role_counts['ACTOR']:,}")
        print(f"  • DIRECTOR: {role_counts['DIRECTOR']:,}")
        print(f"  • PRODUCER: {role_counts['PRODUCER']:,}")
        print(f"  • WRITER: {role_counts['WRITER']:,}")
        print(f"  • COMPOSER: {role_counts['COMPOSER']:,}")
        print("="*80 + "\n")

    def benchmark__test__strong_collaboration_clusters(self):
        """SEM FECHO: Mesma lógica mas MUITO mais complexa"""
        min_collaborations = 2
        print("\n🔍 Running: Strong Collaboration Clusters")
        print(f"  • min_collaborations: {min_collaborations}")
        return ("""
        // Para simular triângulo com colaborações fortes (>= 2 filmes juntos)
        
        // Primeira aresta: a-b com pelo menos 2 filmes
        MATCH (a:Person)-[]->(m1:Movie)<-[]-(b:Person)
        WHERE a.id < b.id
        WITH a, b, COUNT(DISTINCT m1) as ab_strength
        WHERE ab_strength >= $min_collaborations
        
        // Segunda aresta: b-c com pelo menos 3 filmes  
        MATCH (b)-[]->(m2:Movie)<-[]-(c:Person)
        WHERE c.id > b.id AND c <> a
        WITH a, b, c, ab_strength, COUNT(DISTINCT m2) as bc_strength
        WHERE bc_strength >= $min_collaborations
        
        // Terceira aresta: c-a com pelo menos 3 filmes
        MATCH (c)-[]->(m3:Movie)<-[]-(a)
        WITH a, b, c, ab_strength, bc_strength, COUNT(DISTINCT m3) as ca_strength
        WHERE ca_strength >= $min_collaborations
        
        RETURN a.name as person1, 
            b.name as person2, 
            c.name as person3,
            (ab_strength + bc_strength + ca_strength) as total_cluster_strength
        ORDER BY total_cluster_strength DESC
        LIMIT 15;
        """, {"min_collaborations": min_collaborations})

    def benchmark__test__complex_categorical_analytics(self):
        """Multi-dimensional filtering - INEFFICIENT with property scanning"""
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
        print("\n🔍 Running: Cross-Role Workforce Analysis")
        return ("""
        // Análise dos principais salários por função
        MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
        RETURN p.name, 'ACTOR' as role, r.salary as salary
        UNION
        MATCH (p:Person)-[r:DIRECTED]->(m:Movie)  
        RETURN p.name, 'DIRECTOR' as role, r.salary as salary
        UNION
        MATCH (p:Person)-[r:PRODUCED]->(m:Movie)
        RETURN p.name, 'PRODUCER' as role, r.salary as salary
        UNION
        MATCH (p:Person)-[r:WROTE]->(m:Movie)
        RETURN p.name, 'WRITER' as role, r.salary as salary
        UNION
        MATCH (p:Person)-[r:COMPOSED_FOR]->(m:Movie)
        RETURN p.name, 'COMPOSER' as role, r.salary as salary
        ORDER BY salary DESC
        LIMIT 30;
        """, {})

    def benchmark__test__relationship_property_mining(self):
        """OTIMIZADA: Usa dados desnormalizados"""
        min_salary = 1500000
        
        print("\n🔍 Running: Relationship Property Mining (Optimized)")
        print(f"  • min_salary: ${min_salary:,}")
        
        return ("""
        // RÁPIDA: Usa índices em propriedades desnormalizadas
        MATCH (p:Person)
        WHERE p.avg_lead_salary >= $min_salary AND p.lead_roles_count >= 2
        RETURN p.name, p.popularity,
               p.lead_roles_count as high_paid_lead_roles,
               p.avg_lead_salary as avg_lead_salary,
               p.total_acting_roles as total_acting_roles,
               p.avg_acting_salary as overall_avg_salary
        ORDER BY p.avg_lead_salary DESC
        LIMIT 20;
        """, {"min_salary": min_salary})
    
    def benchmark__test__workforce_salary_analytics(self):
        """Analytics salariais da força de trabalho - SEM índices em relationships"""
        min_salary = 1000000
        
        print("\n🔍 Running: Workforce Salary Analytics")
        print(f"  • min_salary: ${min_salary:,}")
        
        return ("""
        // Analytics complexas sem índices - requer scan
        MATCH (p:Person)-[r]->(m:Movie)
        WHERE r.salary >= $min_salary
        WITH p, type(r) as role, r.salary as salary
        WITH p, role, 
            COUNT(*) as role_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        WHERE role_count >= 2
        RETURN p.name, p.popularity, role, role_count, avg_salary, max_salary
        ORDER BY avg_salary DESC
        LIMIT 20;
        """, {"min_salary": min_salary})
    
    def benchmark__test__denormalized_genre_performance(self):
        """OTIMIZADA: Query que USA dados desnormalizados REAIS em Genre"""
        print("\n🔍 Running: Denormalized Genre Performance (Optimized)")
        return ("""
        // RÁPIDA: Dados pré-computados com índices
        MATCH (g:Genre)
        WHERE g.avg_budget >= 20000000 
        AND g.avg_rating >= 7.0
        RETURN g.name, g.avg_rating, g.avg_budget, g.movie_count
        ORDER BY g.avg_rating DESC, g.avg_budget DESC
        LIMIT 10;
        """, {})
    
    def benchmark__test__complex_country_network_base(self):
        """BASE VERSION - Complex country-based network analysis using property scans"""
        print("\n🔍 Running: Complex Country Network Base")
        return ("""
        // Find countries with strong domestic collaboration networks
        MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
        WHERE p1.country = p2.country  // EXPENSIVE: Property comparison on all Person nodes
        WITH p1.country as country_code, 
            COUNT(DISTINCT m) as domestic_movies,
            COUNT(DISTINCT p1) as unique_actors
        
        // Find cross-country collaborations for these countries  
        MATCH (p3:Person)-[:ACTED_IN]->(m2:Movie)<-[:ACTED_IN]-(p4:Person)
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